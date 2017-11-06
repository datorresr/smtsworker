#!/usr/bin/python
import configparser
import MySQLdb
import binascii
import subprocess as sp
import smtplib
import os
from smtplib import SMTPException

# We'll initialize the config variable.
config = configparser.ConfigParser()

# We'll instruct the config to read the .ini file.
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)

# Now we will read the connection configuration to the Amazon RDS Service.
db_host = config['db.amazon']['host']
db_port = config['db.amazon']['port']
db_user = config['db.amazon']['user']
db_passw = config['db.amazon']['passw']
db_name = config['db.amazon']['db']

# We create a connection to the database and create a cursor
dbConnection = MySQLdb.connect(
	host = db_host,
	port = int(db_port),
	user = db_user, 
	passwd = db_passw,
	db = db_name)
dbCursor = dbConnection.cursor()
# Now we can execute any Query we need.
dbCursor.execute("SELECT * FROM videos WHERE convirtiendo=0 AND estado=0")

# We read the email conf.
e_address = config['smtp.amazon']['address']
e_port = config['smtp.amazon']['port']
e_user = config['smtp.amazon']['user']
e_passw = config['smtp.amazon']['passw']
e_domain = config['DEFAULT']['domain']

# Create the message to be sent once the convertion is finished.
message = """From: From Person <smarttoolsg5@gmail.com>
To: To Person <smarttoolsg5v2@gmail.com>
MIME-Version: 1.0
Content-type: text/html
Subject: SMTP HTML e-mail test

Su video ha sido convertido de manera exitosa. Puedes verlo ingresando a la p&aacutegina del concurso.

"""

# We iterate over the videos to make the convertion
rows = dbCursor.fetchall()
folder = config['DEFAULT']['folder']
print(dbCursor.rowcount)
for row in rows:
	v_id = row[0]
	dbCursor.execute("UPDATE videos SET convirtiendo=1 WHERE id={0}".format(v_id))
	dbConnection.commit()

for row in rows:
	v_id = row[0]
	vid_name = row[6]
	source_url = folder+vid_name
	out_url = source_url.rsplit('.', 1)[0]+".mp4"
	command = "ffmpeg -i {0} -f mp4 -vcodec h264 -c:a aac -strict -2 {1} -y"
	vid_out = row[6].rsplit('.', 1)[0]+".mp4"
	print(vid_out)
	print(command)
	os.system(command.format(source_url, out_url))
	dbCursor.execute("UPDATE videos SET estado=1 WHERE id={0}".format(v_id))
	dbCursor.execute("UPDATE videos SET video_out='{1}' WHERE id={0}".format(v_id, vid_out))
	dbConnection.commit()
	# We send the email
	try:
		receiver = row[3]
		# Create the message variables
		sender = 'smarttoolsg5@gmail.com'
		receivers = ['smarttoolsg5v2@gmail.com']
		smtpObj = smtplib.SMTP(e_address, int(e_port), e_domain)
		#smtpObj.set_debuglevel(1)
		smtpObj.starttls()
	   	smtpObj.login(e_user, e_passw)
	   	smtpObj.sendmail(sender, receivers, message)
	   	smtpObj.quit()
	   	print "Successfully sent email"
	except SMTPException:
	   	print "Error: unable to send email"

dbConnection.close()
