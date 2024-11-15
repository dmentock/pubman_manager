from flask import Flask
from flask_mail import Mail
from flask_login import LoginManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import logging
import yaml
from pathlib import Path

from routes import doi_parser
from misc import update_cache
from pubman_manager import PubmanExtractor, create_sheet

app = Flask(__name__)
mail = None

app.secret_key = 'your_secret_key'  # Replace with a secure key

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

def periodic_task():
    with app.app_context():
        logging.info("Updating Pubman Cache + Talks template")
        with open(Path(__file__).parent / 'users.yaml', 'r') as f:
            users = yaml.safe_load(f)
        for org_id in {users[user]['org_id'] for user in users.keys()}:
            update_cache(org_id)

        print("Periodic task executed!")

scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(
    func=periodic_task,
    trigger=IntervalTrigger(hours=24),
    id='daily_task',
    name='Run periodic_task every 24 hours',
    replace_existing=True,
)

import atexit
atexit.register(lambda: scheduler.shutdown())

from routes import *
if __name__ == '__main__':
    app.config['MAIL_SERVER'] = 'smtp.gmail.com'  # Replace with your provider
    app.config['MAIL_PORT'] = 587
    app.config['MAIL_USE_TLS'] = True
    app.config['MAIL_USERNAME'] = 'your_email@example.com'
    app.config['MAIL_PASSWORD'] = 'your_email_password'
    mail = Mail(app)
    app.run(host='0.0.0.0', debug=True)
