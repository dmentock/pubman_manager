from flask import Flask
from flask_mail import Mail
from flask_login import LoginManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import yaml
from pathlib import Path

# from routes import doi_parser
from misc import update_cache, send_publications, run_periodic_task
from pubman_manager import PubmanExtractor, create_sheet, PubmanBase, DOIParser
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
        run_periodic_task()
        logging.info("Periodic task executed!")
scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(
    func=periodic_task,
    trigger=CronTrigger(hour=12, minute=00),
    id='daily_task',
    name='Send new publications',
    replace_existing=True,
)

import atexit
atexit.register(lambda: scheduler.shutdown())

from routes import *
if __name__ == '__main__':
    # run_periodic_task()
    app.run(host='0.0.0.0', debug=True)
