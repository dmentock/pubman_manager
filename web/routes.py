# routes.py
from flask import render_template, redirect, url_for, request, flash, send_file, g, send_from_directory
from flask_login import login_user, login_required, logout_user, current_user
from app import app, login_manager
from user import User
from pubman_manager import DOIParser, PubmanBase, PubmanCreator, PUBLICATIONS_DIR
import datetime
import logging
import os
import traceback
import tempfile
import pandas as pd

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
            auth_token, user_id = PubmanCreator.login(username, form.password.data)
            print("keke")
            pubman_creator = PubmanCreator(auth_token=auth_token, user_id=user_id)
            doi_parser = DOIParser(pubman_creator, logging_level=logging.DEBUG)
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

@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    if request.method == 'POST':
        dois = request.form.get('dois').splitlines()
        dois = [doi.strip() for doi in dois if doi.strip()]
        try:
            print("dois",dois)
            df_dois_overview = doi_parser.filter_dois(dois)
            dois_data = doi_parser.collect_data_for_dois(df_dois_overview, force=True)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            temp_file_path = temp_file.name  # Store the file path
            try:
                doi_parser.write_dois_data(temp_file_path, dois_data)
                return send_file(temp_file_path, as_attachment=True, download_name=os.path.basename(temp_file_path), mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            finally:
                temp_file.close()
        except Exception as e:
            error_message = traceback.format_exc()
            flash(f'Error: {error_message}')

    return render_template('dashboard.html')

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
    if 'pub_file' not in request.files:
        flash('No file uploaded.')
        return redirect(url_for('dashboard'))
    file = request.files['pub_file']
    if file.filename == '':
        flash('No file selected.')
        return redirect(url_for('dashboard'))
    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
        g.temp_file_path = temp_file.name  # Store the file path in `g`
        file.save(g.temp_file_path)        # Save the uploaded file to the temporary file
        pubman_creator.create_publications(g.temp_file_path, overwrite=True, submit_items=False)
        flash(f'Publications created from uploaded file: {file.filename}')

    except Exception as e:
        error_message = traceback.format_exc()
        flash(f'Error: {error_message}')

    return redirect(url_for('dashboard'))

@app.route('/download_publication_template')
@login_required
def download_publication_template():
    return send_file(PUBLICATION_TEMPLATE_PATH, as_attachment=True, download_name="publication_template.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Route to download the talks template
@app.route('/download_talks_template')
@login_required
def download_talks_template():
    return send_file(TALKS_TEMPLATE_PATH, as_attachment=True, download_name="talks_template.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Route to upload a file for creating talks
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
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
        g.temp_file_path = temp_file.name  # Store the file path in `g`
        file.save(g.temp_file_path)        # Save the uploaded file to the temporary file
        api.create_talks(g.temp_file_path, create_items=True, submit_items=True, overwrite=True)
        flash(f'Talks created from uploaded file: {file.filename}')
    except Exception as e:
        error_message = traceback.format_exc()
        flash(f'Error: {error_message}')
    return redirect(url_for('dashboard'))