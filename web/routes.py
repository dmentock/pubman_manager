# routes.py
from flask import render_template, redirect, url_for, request, flash, send_file, g, jsonify
from flask_login import login_user, login_required, logout_user, current_user
import logging
import os
import traceback
import tempfile
import pandas as pd
import yaml
from pathlib import Path

from app import app, login_manager
from user import User

from misc import update_cache, send_test_mail_, send_author_publications, get_file_for_dois, get_user_dois
from pubman_manager import DOIParser, PubmanExtractor, PubmanCreator, TALKS_DIR, PUBMAN_CACHE_DIR

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
            print("oops")
            username = form.username.data
            auth_token, user_id, = PubmanCreator.login(username, form.password.data)
            user_info = PubmanCreator.get_user_info(auth_token, user_id)
            ctx_id = user_info['ctx_id']
            org_id = user_info['org_id']
            with open(Path(__file__).parent / 'users.yaml', 'r') as f:
                users = yaml.safe_load(f)
            if org_id not in {user['org_id'] for user in users.values()}:
                update_cache(org_id)
                users[user_id] = {
                    'org_id': org_id,
                    'ctx_id': ctx_id,
                    'tracked_authors': []
                }
                with open('users.yaml', 'w') as f:
                    yaml.dump(users, f)

            print("keke")
            pubman_creator = PubmanCreator(auth_token=auth_token, user_id=user_id)
            doi_parser = DOIParser(pubman_creator)
            user = User(username)
            login_user(user)
            return redirect(url_for('dashboard'))
        except Exception as e:
            error_message = traceback.format_exc()
            flash(f'Error: {error_message}')
    return render_template('login.html', form=form)

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
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        file.save(temp_file.name)
        pubman_creator.create_publications(temp_file.name, overwrite=True, submit_items=False)
        flash('Publications created successfully.')
    except Exception as e:
        flash(f"Error creating publications: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/dashboard', methods=['GET'])
@login_required
def dashboard():
    with open(Path(__file__).parent / 'users.yaml', 'r') as f:
        users = yaml.safe_load(f)
    tracked_authors = users[pubman_creator.user_id]['tracked_authors']
    tracked_authors_str = "\n".join(tracked_authors)
    ignored_dois = users[pubman_creator.user_id].get('ignored_dois', [])
    ignored_dois_str = "\n".join(ignored_dois)
    return render_template('dashboard.html', tracked_authors=tracked_authors_str, ignored_dois=ignored_dois_str)

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
    print("authors", authors)
    try:
        with open(Path(__file__).parent / 'users.yaml', 'r') as f:
            users = yaml.safe_load(f)
        users[pubman_creator.user_id]['tracked_authors'] = authors
        with open(Path(__file__).parent / 'users.yaml', 'w') as f:
            yaml.dump(users, f)
        flash('Tracked authors updated successfully.')
    except Exception as e:
        flash(f"Error saving authors: {traceback.format_exc()}")
    if action == 'send':
        try:
            flash('Fetching new publications of authors, please wait...')
            new_publication_dois = get_user_dois(pubman_creator.user_id, doi_parser)
            send_author_publications(new_publication_dois, pubman_creator.user_email, doi_parser)
        except Exception as e:
            flash(f"Error updating and sending new publications: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/ignored_dois_str', methods=['POST'])
@login_required
def ignored_dois_str():
    ignored_dois = request.form.get('ignored_dois', '').splitlines()
    ignored_dois = [author.strip() for author in ignored_dois if author.strip()]
    print("ignored_dois", ignored_dois)
    try:
        with open(Path(__file__).parent / 'users.yaml', 'r') as f:
            users = yaml.safe_load(f)
        users[pubman_creator.user_id]['ignored_dois'] = ignored_dois
        with open(Path(__file__).parent / 'users.yaml', 'w') as f:
            yaml.dump(users, f)
        flash('DOIS to ignore updated successfully.')
    except Exception as e:
        flash(f"Error saving DOIS to ignore: {traceback.format_exc()}")
    return redirect(url_for('dashboard'))

@app.route('/download_talks_template_public', methods=['GET'])
def download_talks_template_public():
    try:
        return send_file(
            TALKS_DIR / 'Template_Talks.xlsx',
            as_attachment=True,
            download_name="talks_template.xlsx",
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
