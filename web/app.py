from flask import Flask
from flask_login import LoginManager

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with a secure key

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

from routes import *

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

