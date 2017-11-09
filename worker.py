#!/usr/bin/python
# -*- coding: utf-8 -*-
import multiprocessing, time, os, configparser, binascii, subprocess as sp, smtplib, json, boto3, MySQLdb, requests 
from smtplib import SMTPException
from multiprocessing import Pool, TimeoutError
from iron_mq import *

# We'll initialize the config variable.
config = configparser.ConfigParser()

# We'll instruct the config to read the .ini file.
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)
print ("Ejecutando Worker")
print ("Ejecutando Worker1")

# We will read the connection configuration to the Amazon RDS Service.
db_host = config['db.amazon']['host']
db_port = config['db.amazon']['port']
db_user = config['db.amazon']['user']
db_passw = config['db.amazon']['passw']
db_name = config['db.amazon']['db']

ironmq = IronMQ(host=os.environ['IRON_HOST'], project_id=os.environ['IRON_ID'], token=os.environ['IRON_TOKEN'])
queue = ironmq.queue(os.environ['IRON_QUEUE'])

# We create a connection to the database and create a cursor
#dbConnection = MySQLdb.connect(
#	host = db_host,
#	port = int(db_port),
#	user = db_user, 
#	passwd = db_passw,
#	db = db_name)
#dbCursor = dbConnection.cursor()

dynamodb = boto3.resource('dynamodb', region_name=os.environ['AWS_REGION'], aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
s3 = boto3.resource('s3', region_name=os.environ['S3_REGION'], aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'])
sqs = boto3.resource('sqs', region_name=os.environ['AWS_REGION'], aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
bucket = s3.Bucket(config['DEFAULT']['bucketS3'])
table = dynamodb.Table('videos')
#queue = sqs.get_queue_by_name(QueueName='smts-videos-queue')
folder = config['DEFAULT']['folder']
folder_s3 = config['DEFAULT']['folderS3']
# We read the email conf.
e_address = config['smtp.amazon']['address']
e_port = config['smtp.amazon']['port']
e_user = config['smtp.amazon']['user']
e_passw = config['smtp.amazon']['passw']
e_domain = config['DEFAULT']['domain']

def main():
	while True:
		print ("hi")
		cpu_count = multiprocessing.cpu_count()
		print (cpu_count)
		videos = get_videos_to_convert(cpu_count)
		with multiprocessing.Pool(cpu_count) as pool:
			pool.map(convert_video, videos)
		time.sleep(50)
	
def get_videos_to_convert(cpu_count):
	videos = []
	#messages = queue.receive_messages(MaxNumberOfMessages=cpu_count)
	msgs = queue.reserve(max=cpu_count, timeout=None, wait=0, delete=False)
	messags = msgs['messages']
	for message in messags:
		video_inf = message['body'] + ';' + message['reservation_id'] + ';' + message['id'] 
		videos.append(video_inf)
	return videos

def convert_video(video):
	video_id, video_concurso_id, video_name, message_reservation_id, message_id = video.split(';')
	print (video_id)
	print (video_concurso_id)
	print (video_name)
	print (folder+video_name)
	print (folder_s3+video_name)
	print (message_reservation_id)
	print (message_id)
	
	# do all prints
	bucket.download_file(folder_s3+video_name, folder+video_name)
	video_output_name = video_name.rsplit('.', 1)[0]+".mp4"
	command = "ffmpeg -i {0}{1} -f mp4 -vcodec h264 -c:a aac -strict -2 {0}{2} -y"
	os.system(command.format(folder, video_name, video_output_name))
	bucket.upload_file(folder+video_output_name, folder_s3+video_output_name)
	#dbCursor.execute("UPDATE videos SET estado=1 WHERE video_source='{0}'".format(video_name))
	key = os.environ['MAIL_KEY']
	sandbox = os.environ['MAIL_SANDBOX']
	recipient = 'smarttoolsg5@gmail.com'

	request_url = 'https://api.mailgun.net/v3/{0}/messages'.format(sandbox)
	request = requests.post(request_url, auth=('api', key), data={
	    'from': 'smarttoolsg5@gmail.com',
	    'to': recipient,
	    'subject': 'Video Convertido',
	    'text': 'Su video ha sido convertido de manera exitosa. Puedes verlo ingresando a la p&aacutegina del concurso.'
	})
	queue.delete(message_id, message_reservation_id)
	# We send the email
	try:
		# Create the message to be sent once the convertion is finished.
		message = """
		Subject: Conversi&oacute;n de video

		Su video ha sido convertido de manera exitosa. Puedes verlo ingresando a la p&aacutegina del concurso.
		"""

		# Create the message variables
		sender = 'smarttoolsg5@gmail.com'
		receivers = ['smarttoolsg5v2@gmail.com']
		smtpObj = smtplib.SMTP(e_address, int(e_port), e_domain)
		smtpObj.set_debuglevel(1)
		smtpObj.starttls()
		smtpObj.login(e_user, e_passw)
		smtpObj.sendmail(sender, receivers, message)
		smtpObj.quit()
		print ("Successfully sent email")
		
		'''queue.delete_messages(
			Entries=[
		    	{
		    		'Id': str(int(round(time.time() * 1000))),
					'ReceiptHandle': message_receipt_handler
				},
			]
		)'''
	except SMTPException:
		print ("Error: unable to send email")


if __name__ == "__main__":
	main()
