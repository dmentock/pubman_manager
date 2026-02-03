# user.py
from flask_login import UserMixin

class User(UserMixin):
    def __init__(self, user_id, username=None):
        self.id = str(user_id)
        self.username = username or str(user_id)
