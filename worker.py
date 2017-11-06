#!/usr/bin/python
# -*- coding: utf-8 -*-
import multiprocessing, time, os, configparser, binascii, subprocess as sp, smtplib, json, boto3  
from smtplib import SMTPException
from multiprocessing import Pool, TimeoutError

# We'll initialize the config variable.
config = configparser.ConfigParser()

# We'll instruct the config to read the .ini file.
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAIVKAI3NDSJDTRSUA', aws_secret_access_key='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')
s3 = boto3.resource('s3', region_name='us-east-2', aws_access_key_id='AKIAJBTATXXUACKXPIRQ', aws_secret_access_key='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')
sqs = boto3.resource('sqs', region_name='us-east-2', aws_access_key_id='AKIAIVKAI3NDSJDTRSUA', aws_secret_access_key='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')
bucket = s3.Bucket('smarttoolsG5')
table = dynamodb.Table('videos')
queue = sqs.get_queue_by_name(QueueName='smts-videos-queue')
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
	messages = queue.receive_messages(MaxNumberOfMessages=cpu_count)
	for message in messages:
		videos.append(message.body)
	return videos

def convert_video(video):
	video_id, video_concurso_id, video_name = video.split('-')
	print (video_id)
	print (video_concurso_id)
	print (video_name)
	print (folder+video_name)
	print (folder_s3+video_name)
	# do all prints
	bucket.download_file(folder_s3+video_name, folder+video_name)
	video_output_name = video_name.rsplit('.', 1)[0]+".mp4"
	command = "ffmpeg -i {0}{1} -f mp4 -vcodec h264 -c:a aac -strict -2 {0}{2} -y"
	os.system(command.format(folder, video_name, video_output_name))
	bucket.upload_file(folder+video_output_name, folder_s3+video_output_name)
	response = table.update_item(
		Key={
	    	'video_id': int(video_id),
	    	'concurso_id': int(video_concurso_id)
		},
		UpdateExpression="set video_info.estado = :e",
		ExpressionAttributeValues={
			':e': 1
		},
		ReturnValues="UPDATED_NEW"
	)
	# We send the email
	try:
		# Create the message to be sent once the convertion is finished.
		message = """From: From Person <smarttoolsg5@gmail.com>
		To: To Person <smarttoolsg5v2@gmail.com>
		MIME-Version: 1.0
		Content-type: text/html
		Subject: SMTP HTML e-mail test

		Su video ha sido convertido de manera exitosa. Puedes verlo ingresando a la p&aacutegina del concurso.

		"""

		# Create the message variables
		sender = 'smarttoolsg5@gmail.com'
		receivers = ['smarttoolsg5v2@gmail.com']
		smtpObj = smtplib.SMTP(e_address, int(e_port), e_domain)
		#smtpObj.set_debuglevel(1)
		smtpObj.starttls()
		smtpObj.login(e_user, e_passw)
		smtpObj.sendmail(sender, receivers, message)
		smtpObj.quit()
		print ("Successfully sent email")
	except SMTPException:
		print ("Error: unable to send email")
	#update_video(video_id, video_concurso_id)
	#send_video_email()

def update_video(video_id, concurso_id):
	response = table.update_item(
		Key={
	    	'video_id': video_id,
	    	'concurso_id': concurso_id
		},
		UpdateExpression="set video_info.estado = :e",
		ExpressionAttributeValues={
			':e': 1
		},
		ReturnValues="UPDATED_NEW"
	)

def send_video_email():
	# We send the email
	try:
		# Create the message to be sent once the convertion is finished.
		message = """From: From Person <smarttoolsg5@gmail.com>
		To: To Person <smarttoolsg5v2@gmail.com>
		MIME-Version: 1.0
		Content-type: text/html
		Subject: SMTP HTML e-mail test

		Su video ha sido convertido de manera exitosa. Puedes verlo ingresando a la p&aacutegina del concurso.

		"""

		# Create the message variables
		sender = 'smarttoolsg5@gmail.com'
		receivers = ['smarttoolsg5v2@gmail.com']
		smtpObj = smtplib.SMTP(e_address, int(e_port), e_domain)
		#smtpObj.set_debuglevel(1)
		smtpObj.starttls()
		smtpObj.login(e_user, e_passw)
		smtpObj.sendmail(sender, receivers, message)
		smtpObj.quit()
		print ("Successfully sent email")
	except SMTPException:
		print ("Error: unable to send email")

def f(source):
	folder = "/home/klyo/Videos/"
	out = source.rsplit('.',1)[0]
	command = "ffmpeg -i {2}{0} -f mp4 -vcodec h264 -c:a aac -strict -2 {2}{1}.mp4 -y"
	os.system(command.format(source, out, folder))

# 	def anotherf():
	#print (os.environ['DOMAIN'])
	#print ("DOMAIN" in os.environ)
	#with multiprocessing.Pool(cpu_count) as pool:
	#	pool.map(f, numbers)

if __name__ == "__main__":
	main()