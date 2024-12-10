from flask import Flask
from flask_mail import Mail
from flask_login import LoginManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import yaml
from pathlib import Path

# from routes import doi_parser
from misc import update_cache
from pubman_manager import PubmanExtractor, create_sheet, PubmanBase
import logging

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
        for org_id in {users[user_id]['org_id'] for user_id in users.keys()}:
            update_cache(org_id)

        for user_id, user_info in users.items():
            pubman_api = PubmanBase()
            parser = DOIParser(pubman_api, logging_level = logging.DEBUG)
            new_publication_dois = get_user_dois(user_id, parser)
            send_author_publications(new_publication_dois, user_info['email'], parser)
        print("Periodic task executed!")

scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(
    func=periodic_task,
    trigger=CronTrigger(hour=1, minute=0),
    id='daily_task',
    name='Send new publications',
    replace_existing=True,
)

import atexit
atexit.register(lambda: scheduler.shutdown())

from routes import *
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
