import os
import zipfile
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import zipfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint




def unzip_csv(file):
    zf = zipfile.ZipFile(file)
    csv_file_name = zf.namelist()[0]
    final_csv_data = zf.open(csv_file_name)
    return final_csv_data

dir_path = os.getcwd()

def saveList(myList,filename):
    # the filename should mention the extension 'npy'
    np.save(filename,myList)
    print("Saved successfully!")



import smtplib
def send_email(from_email, from_email_pass, to_email, subject, body_text):
    #Ports 465 and 587 are intended for email client to email server communication - sending email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    #starttls() is a way to take an existing insecure connection and upgrade it to a secure connection using SSL/TLS.
    server.starttls()
    #Next, log in to the server
    server.login(from_email, from_email_pass)#("prreporting@pivotroots.com", "vwcuvekcfubywxcg")
    msg = MIMEMultipart()
    msg["Subject"] = subject
    body = MIMEText(body_text)
    msg["From"] = from_email#"prreporting@pivotroots.com"
    msg["To"] = to_email#"ekagra.singh@pivotroots.com"
    msg.attach(body)
    server.sendmail(msg["From"], msg["To"],msg.as_string())
    server.quit()

#Send the mail
#tanmay.sankhe@pivotroots.com

def get_uploadparams():
    #Authorizing the API
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://spreadsheets.google.com/feeds'
        ]
    key_file = 'dashbaording-all-clients-e80f9ab53385.json'
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file,scope)
    client = gspread.authorize(creds)
    worksheet = client.open('uploadparams')
    uploadparms_sheet = worksheet.get_worksheet(0)
    upload_params_dict = uploadparms_sheet.get_all_records()
    upload_params = pd.DataFrame.from_dict(upload_params_dict)
    upload_params.set_index('ID', inplace = True)
    upload_params['Last Run Date'] = pd.to_datetime(upload_params['Last Run Date'], format='%Y-%m-%d')
    return uploadparms_sheet, upload_params

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'logzioFormat': {
            'format': '{"additional_field": "value"}',
            'validate': False
        }
    },
    'handlers': {
        'logzio': {
            'class': 'logzio.handler.LogzioHandler',
            'level': 'INFO',
            'formatter': 'logzioFormat',
            'token': 'mvQqDSvpKoEkPUtzZOkdMExuCnXTAQoh',
            'logzio_type': 'python',
            'logs_drain_timeout': 1,
            'url': 'https://listener.logz.io:8071'
        }
    },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['logzio'],
            'propagate': True
        }
    }
}


####
