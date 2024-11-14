from flask import Flask
from flask_login import LoginManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import logging
import yaml

from update_cache import update_cache
from pubman_manager import PubmanExtractor, create_sheet

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with a secure key

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

def periodic_task():
    with app.app_context():
        logging.info("Updating Pubman Cache + Talks template")
        with open('users.yaml', 'r') as f:
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
