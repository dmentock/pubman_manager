# routes.py
from flask import render_template, redirect, url_for, request, flash, send_file, g, jsonify
from flask_login import login_user, login_required, logout_user, current_user
import logging
import os
import traceback
import tempfile
from datetime import datetime
import pandas as pd
import yaml
from pathlib import Path

from app import app, login_manager
from user import User

from misc import update_cache, send_test_mail_, send_author_publications, get_file_for_dois, get_user_dois
from pubman_manager import DOIParser, PubmanExtractor, PubmanCreator, TALKS_DIR, PUBMAN_CACHE_DIR, USER_DATA_DIR

# Initialize your core objects
# pubman_api = None
pubman_creator = None
doi_parser = None

# Mock user database
users = {'admin': {'password': 'password'}}

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    global pubman_creator
    global doi_parser
    from forms import LoginForm
    form = LoginForm()
    if form.validate_on_submit():
        try:
            app.logger.info("Login submitted")
            username = form.username.data
            app.logger.info("Authenticating user %s", username)
            auth_token, user_id, = PubmanCreator.login(username, form.password.data)
            app.logger.info("Auth success for user_id %s", user_id)
            user_info = PubmanCreator.get_user_info(auth_token, user_id)
            app.logger.info("User info retrieved for user_id %s", user_id)
            ctx_id = user_info['ctx_id']
            org_id = user_info['org_id']
            user_name = user_info['user_name']
            app.logger.info("Login context user_name=%s org_id=%s ctx_id=%s", user_name, org_id, ctx_id)
            user_yaml_path = USER_DATA_DIR / f"user_{user_id}.yaml"
            if not user_yaml_path.exists():
                app.logger.info("User yaml missing, updating cache for org_id %s", org_id)
                update_cache(user_id, [org_id])
                app.logger.info("Cache updated for org_id %s", org_id)
                user_yaml_path.parent.mkdir(parents=True, exist_ok=True)
                with user_yaml_path.open("w", encoding="utf-8") as f:
                    yaml.safe_dump({"tracked_authors": [], "department_org_ids": [org_id]}, f, sort_keys=False)
                app.logger.info("Created user yaml at %s", user_yaml_path)

            pubman_creator = PubmanCreator(auth_token=auth_token, user_id=user_id)
            doi_parser = DOIParser(pubman_creator)
            user = User(user_id, username=username)
            login_user(user)
            app.logger.info("User %s logged in", username)
            return redirect(url_for('dashboard'))
        except Exception as e:
            error_message = traceback.format_exc()
            flash(f'Error: {error_message}')
    return render_template('login.html', form=form)


def _ensure_pubman_creator():
    global pubman_creator
    global doi_parser
    if pubman_creator is None:
        pubman_creator = PubmanCreator()
        doi_parser = DOIParser(pubman_creator)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/download_publications_excel', methods=['POST'])
@login_required
def download_publications_excel():
    # Extract DOIs from the form
    dois = request.form.get('dois', '').splitlines()
    dois = [doi.strip() for doi in dois if doi.strip()]
    if not dois:
        flash('No valid DOIs provided.')
        return redirect(url_for('dashboard'))
    try:
        temp_file_path = get_file_for_dois(dois, doi_parser)
        return send_file(temp_file_path, as_attachment=True, download_name="publications.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        flash(f"Error processing DOIs: {traceback.format_exc()}")
        return redirect(url_for('dashboard'))

@app.after_request
def delete_temp_file(response):
    # Get the temp_file_path from the response context
    temp_file_path = getattr(g, 'temp_file_path', None)
    if temp_file_path and os.path.exists(temp_file_path):
        try:
            os.remove(temp_file_path)  # Delete the file after the response
        except Exception as e:
            app.logger.error(f"Error deleting temp file {temp_file_path}: {e}")
    return response

@app.route('/create_publications', methods=['POST'])
@login_required
def create_publications():
    file = request.files.get('pub_file')
    if not file or file.filename == '':
        flash('No file selected.')
        return redirect(url_for('dashboard'))

    try:
        _ensure_pubman_creator()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        file.save(temp_file.name)
        pubman_creator.create_publications(temp_file.name, overwrite=True, submit_items=False)
        flash('Publications created successfully.')
    except Exception as e:
        flash(f"Error creating publications: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

def _load_user_dashboard_data(user_id: str) -> tuple[str, str, str, str]:
    user_yaml_path = USER_DATA_DIR / f"user_{user_id}.yaml"
    if not user_yaml_path.exists():
        return "", "", "", ""
    with user_yaml_path.open("r", encoding="utf-8") as f:
        user_data = yaml.safe_load(f) or []
    if isinstance(user_data, dict):
        tracked_authors = user_data.get("tracked_authors", [])
        department_org_ids = user_data.get("department_org_ids", [])
    else:
        tracked_authors = user_data
        department_org_ids = []
    tracked_authors_str = "\n".join(tracked_authors)
    department_org_ids_str = "\n".join(department_org_ids)
    ignored_dois_path = USER_DATA_DIR / f"user_{user_id}_ignored_dois.yaml"
    if ignored_dois_path.exists():
        with ignored_dois_path.open("r", encoding="utf-8") as f:
            ignored_dois = yaml.safe_load(f) or []
    else:
        ignored_dois = []
    ignored_dois_str = "\n".join(ignored_dois)
    cache_dir = PUBMAN_CACHE_DIR / f"user_{user_id}"
    if cache_dir.exists():
        last_modified = datetime.fromtimestamp(cache_dir.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    else:
        last_modified = "N/A"
    return tracked_authors_str, ignored_dois_str, department_org_ids_str, last_modified

def _resolve_user_id() -> str:
    raw_id = getattr(current_user, "id", "")
    raw_id = str(raw_id) if raw_id is not None else ""
    if raw_id.startswith("user_"):
        return raw_id.replace("user_", "", 1)
    if raw_id.isdigit():
        return raw_id
    if pubman_creator and getattr(pubman_creator, "user_id", None):
        return str(pubman_creator.user_id)
    return raw_id

@app.route('/dashboard', methods=['GET'])
@login_required
def dashboard():
    tracked_authors_str, ignored_dois_str, department_org_ids_str, cache_last_modified = _load_user_dashboard_data(_resolve_user_id())
    return render_template(
        'dashboard.html',
        tracked_authors=tracked_authors_str,
        ignored_dois=ignored_dois_str,
        department_org_ids=department_org_ids_str,
        cache_last_modified=cache_last_modified,
    )

@app.route('/send_test_mail', methods=['POST'])
@login_required
def send_test_mail():
    try:
        send_test_mail_(pubman_creator.user_email)
        flash(f'Test mail sent to {pubman_creator.user_email}')
        return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f'Error: {traceback.format_exc()}')
        return redirect(url_for('dashboard'))

@app.route('/set_or_send_tracked_authors', methods=['POST'])
@login_required
def set_or_send_tracked_authors():
    action = request.form.get('action')
    authors = request.form.get('tracked_authors', '').splitlines()
    authors = [author.strip() for author in authors if author.strip()]
    try:
        user_yaml_path = USER_DATA_DIR / f"user_{_resolve_user_id()}.yaml"
        user_yaml_path.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if user_yaml_path.exists():
            with user_yaml_path.open("r", encoding="utf-8") as f:
                existing = yaml.safe_load(f) or {}
        if not isinstance(existing, dict):
            existing = {}
        existing["tracked_authors"] = authors
        with user_yaml_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(existing, f, sort_keys=False)
        flash('Tracked authors updated successfully.')
    except Exception as e:
        flash(f"Error saving authors: {traceback.format_exc()}")
    if action == 'send':
        try:
            flash('Fetching new publications of authors, please wait...')
            send_mode = request.form.get("send_mode", "new")
            force = send_mode == "all"
            new_publication_dois = get_user_dois(_resolve_user_id(), doi_parser, force=force)
            send_author_publications(new_publication_dois, pubman_creator.user_email, doi_parser)
        except Exception as e:
            flash(f"Error updating and sending new publications: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/ignored_dois_str', methods=['POST'])
@login_required
def ignored_dois_str():
    ignored_dois = request.form.get('ignored_dois', '').splitlines()
    ignored_dois = [author.strip() for author in ignored_dois if author.strip()]
    try:
        ignored_dois_path = USER_DATA_DIR / f"user_{_resolve_user_id()}_ignored_dois.yaml"
        ignored_dois_path.parent.mkdir(parents=True, exist_ok=True)
        with ignored_dois_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(ignored_dois, f)
        flash('DOIS to ignore updated successfully.')
    except Exception as e:
        flash(f"Error saving DOIS to ignore: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/set_department_org_ids', methods=['POST'])
@login_required
def set_department_org_ids():
    org_ids = request.form.get('department_org_ids', '').splitlines()
    org_ids = [org_id.strip() for org_id in org_ids if org_id.strip()]
    try:
        user_yaml_path = USER_DATA_DIR / f"user_{_resolve_user_id()}.yaml"
        user_yaml_path.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if user_yaml_path.exists():
            with user_yaml_path.open("r", encoding="utf-8") as f:
                existing = yaml.safe_load(f) or {}
        if not isinstance(existing, dict):
            existing = {}
        existing["department_org_ids"] = org_ids
        with user_yaml_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(existing, f, sort_keys=False)
        flash('Department org IDs updated successfully.')
    except Exception as e:
        flash(f"Error saving department org IDs: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/update_pure_data', methods=['POST'])
@login_required
def update_pure_data():
    try:
        user_id = _resolve_user_id()
        user_yaml_path = USER_DATA_DIR / f"user_{user_id}.yaml"
        if not user_yaml_path.exists():
            flash('User yaml not found.')
            return redirect(url_for('dashboard'))
        with user_yaml_path.open("r", encoding="utf-8") as f:
            user_data = yaml.safe_load(f) or {}
        if not isinstance(user_data, dict):
            flash('Invalid user yaml format.')
            return redirect(url_for('dashboard'))
        org_ids = user_data.get("department_org_ids", [])
        if not org_ids:
            flash('No department org IDs configured.')
            return redirect(url_for('dashboard'))
        update_cache(user_id, org_ids)
        flash('PuRe data updated successfully.')
    except Exception as e:
        flash(f"Error updating PuRe data: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/download_talks_template_public', methods=['GET'])
def download_talks_template_public():
    try:
        _ensure_pubman_creator()
        file_path = TALKS_DIR / f"Template_Talks_{pubman_creator.org_id}.xlsx"
        if not file_path.exists():
            flash("Please re-generate pubman cache data once")
            return redirect(url_for('dashboard'))
        return send_file(
            file_path,
            as_attachment=True,
            download_name=file_path.name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        error_message = traceback.format_exc()
        app.logger.error(f"Error serving public talks template: {error_message}")
        return "Error serving the file", 500

@app.route('/create_talks', methods=['POST'])
@login_required
def create_talks():
    if 'talks_file' not in request.files:
        flash('No file uploaded.')
        return redirect(url_for('dashboard'))

    file = request.files['talks_file']
    if file.filename == '':
        flash('No file selected.')
        return redirect(url_for('dashboard'))

    try:
        # Save the uploaded file to a temporary location
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
        temp_file_path = temp_file.name  # Store the temp file path
        g.temp_file_path = temp_file_path  # Save the file path for later cleanup
        file.save(temp_file_path)

        # Process the uploaded file to create talks
        pubman_creator.create_talks(temp_file_path, create_items=True, submit_items=True, overwrite=True)
        flash(f'Talks created successfully from file: {file.filename}')
    except Exception as e:
        error_message = traceback.format_exc()
        flash(f'Error: {error_message}')
    return redirect(url_for('dashboard'))
