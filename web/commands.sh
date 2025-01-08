Run your Flask app with nohup:

bash
Code kopieren
nohup python3 app.py > flask_app.log 2>&1 &
The & at the end runs the command in the background.

The output and errors are redirected to flask_app.log.

Verify:
Check if the app is running:
bash
Code kopieren
ps aux | grep app.py
View the logs:
bash
Code kopieren
tail -f flask_app.log