# user.py
from flask_login import UserMixin

class User(UserMixin):
    def __init__(self, username):
        self.id = username
