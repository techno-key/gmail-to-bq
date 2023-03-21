from fileinput import filename
import imaplib
import pandas as pd,traceback,re
from datetime import datetime, timedelta,date
import pytz
import email
import io,zipfile
from google.oauth2 import service_account
from google.cloud import bigquery
from utils import unzip_csv , send_email, get_uploadparams , LOGGING,smytten_table_mapping,column_name_formatting
import pandas_gbq
import csv
from urllib.request import urlopen


uploadparms_sheet, upload_params = get_uploadparams()

#logging integration
import logging
import logging.config
import ecs_logging

logging.config.dictConfig(LOGGING)
handler = logging.FileHandler('formatted.json')
logger = logging.getLogger("Gmail to BigQuery Common")
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)



class Email_To_BQ():
    def __init__(self, start_date, param_ID):
        start_date = (date.today() - timedelta(0)).strftime('%d-%b-%Y')
        self.start_date = start_date
        self.param_ID = param_ID
        self.mail = imaplib.IMAP4_SSL('imap.gmail.com')
        self.mail.login(upload_params.loc[param_ID,'Username'], upload_params.loc[param_ID,'Password'])
        self.mail.select("inbox") # connect to inbox.
        self.search_query = '(FROM "'+upload_params.loc[param_ID,'From_Email']+'" SUBJECT "'+upload_params.loc[param_ID,'Search Query']+'") SINCE "' + start_date + '"'
        #'from:('+upload_params.loc[param_ID,'From_Email']+') SUBJECT:('+upload_params.loc[param_ID,'Search Query']+') after :"' + start_date + '"'
        try:
            self.already_ingested_files = list(pd.read_csv('Ingested_files/' + upload_params.loc[param_ID, 'Ingested File Name'])['filename'])        # column name in ingested file
        except:
            self.already_ingested_files = []

    def our_id_list(self):
        result, data = self.mail.search(None, self.search_query)
        ids = data[0] # data is a list.
        id_list = ids.split() # ids is a space separated string
        print("id_list = ", id_list)
        return id_list

    def run(self):
        id_list = self.our_id_list()

        # for email_id in reversed(id_list):
        for email_id in id_list:
            result, data = self.mail.fetch(email_id, "(RFC822)")
            raw_email = data[0][1]
            
            datestring = email.message_from_bytes(raw_email)['date']        # extracting date and time of email
            datetime_obj = email.utils.parsedate_to_datetime(datestring)
            email_date = datetime_obj#.date()
            print('Email date: ',email_date)

            raw_email_string = raw_email.decode('utf-8')
            email_message = email.message_from_string(raw_email_string)
            #print(email_message)
            for header in [ 'subject' ]:
                subject_ = email_message[header]
                print("jhds:-",subject_)


            for part in email_message.walk():  # goes inside the mail to check for attachment

                if part.get_content_type() == 'text/plain': 
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue

                    if upload_params.loc[self.param_ID, 'File Type'] in ['url_zip','url_csv','url_excel']:      # if data is in link, search for link in email body
                        plain_text = part.get_payload()
                        word_list = plain_text.split()
                        file_url = [k for k in word_list if ('https:' in k) or ('http:' in k)][0].lstrip('<').rstrip('>')
                        print('file url:',file_url)
                        fileName = file_url.split('/')[-1]

                    else:
                        fileName = part.get_filename()
                    print("fileName = ", fileName)

                    if fileName not in self.already_ingested_files & (str(upload_params.loc[param_ID, 'Attachment File Name']).lower() in str(fileName).lower()):
                        print(fileName)
                        #print(fileName.split(".csv")[0][-10:])
                        try:
                            if upload_params.loc[self.param_ID, 'File Type'] in ['url_zip','url_csv','url_excel']:
                                #print("urllll:-",file_url)
                                resp = urlopen(file_url)
                                file_bytes = io.BytesIO(resp.read())
                            else:
                                file_bytes = io.BytesIO(part.get_payload(decode=True))

                            if upload_params.loc[self.param_ID, 'File Type'] in ['zip','url_zip']:
                                csv_file = unzip_csv(file_bytes)  # unzipping the file returning final csv data
                                print("filesssss:-",csv_file)
                                df = pd.read_csv(csv_file)
                            elif upload_params.loc[self.param_ID, 'File Type'] in ['csv','url_csv']:
                                csv_file = file_bytes
                                df = pd.read_csv(csv_file)
                            elif upload_params.loc[self.param_ID, 'File Type'] in ['excel','url_excel']:
                                excel_file = file_bytes
                                df = pd.read_excel(excel_file)
                            else:
                                print("Invalid file type")

                            df['updated_datetime'] = email_date
                            df = df.astype(str)
                            df = df.fillna('')
                            print(df.head())
                            credentials = service_account.Credentials.from_service_account_file('Cred_files/' + upload_params.loc[self.param_ID,'Credential Json File'],)
                            bq_client = bigquery.Client(credentials=credentials, project=upload_params.loc[self.param_ID,'Project ID'])

                            #print('columns = ', df.columns)
                            df = df.loc[:,upload_params.loc[self.param_ID,'Input Column Names'].split(',') + ['updated_datetime']]
                            df.columns = upload_params.loc[self.param_ID,'Destination Column Names'].split(',') + ['updated_datetime']
                            
                            df['updated_datetime'] = df['updated_datetime'].apply(lambda x: pd.Timestamp(x, tz='Asia/Kolkata'))    #converting string back to timestamp
                            
                            # Uploading data to gbq
                            pandas_gbq.to_gbq(df, upload_params.loc[self.param_ID,'Destination Table'], upload_params.loc[self.param_ID,'Project ID'],
                                                credentials=credentials, if_exists=upload_params.loc[self.param_ID, 'IF Exists'])

                            logger.info(f"successfully ingested {len(df)} rows in {upload_params.loc[self.param_ID,'Destination Table']}")

                            print("  ", df.head())


                            ing_file = upload_params.loc[self.param_ID, 'Ingested File Name']
                            with open('Ingested_files/'+ing_file, 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow([fileName])

                            upload_params.loc[self.param_ID, 'Last Run Date'] = today_date.strftime('%Y-%m-%d')      #updating last run date
                            upload_params.loc[self.param_ID, 'Last Run Hour'] = hour_now                             #updating last run hour


                        except Exception as e:
                            traceback.print_exc()
                            logger.exception('ERROR')
                            print(logger.exception('Error'))
                            dest_table = upload_params.loc[self.param_ID,'Destination Table']
                            send_email(from_email=upload_params.loc[self.param_ID,'Error_From_Email'],                   #send an email if there is an error in uploading
                                        from_email_pass=upload_params.loc[self.param_ID,'Error_From_Email_Password'],
                                        to_email=upload_params.loc[self.param_ID,'Error_To_Email'],
                                        subject='Error in Uploading file(s)', 
                                        body_text=f'There was an error in uploading the file to {dest_table} : {e}')
                            print('--Error--\n')
                            print(e)

                elif part.get_content_type() == 'text/html':

                    if upload_params.loc[self.param_ID, 'File Type'] in ['url_zip_files']:      # if data is in link, search for link in email body
                        plain_text = part.get_payload()

                        word_list = plain_text.replace('=\r\n','').replace('<br>','')  #.split(" ")

                        ls = upload_params.loc[self.param_ID,'File Names'].split(',')   #filter Moengage reports by specific client reports name

                        try:
                            for l in ls:
                                if l in word_list:
                                    print("word_list:-", word_list)
                                    word_list = word_list.split(" ")
                                    file_url = [k for k in word_list if ('https:' in k) or ('file' in k)]
                                    file_url = [p.replace('=\r\n', '').replace('<br>', '') for p in file_url if 'file' in p][0]
                                    print('file_url:-', file_url)

                                else:
                                    file_url = None

                                if file_url is not None:
                                    if upload_params.loc[self.param_ID, 'File Type'] in ['url_zip_files']:
                                        resp = urlopen(file_url)
                                        file_bytes = io.BytesIO(resp.read())
                                    else:
                                        file_bytes = io.BytesIO(part.get_payload(decode=True))

                                    if upload_params.loc[self.param_ID, 'File Type'] in ['url_zip_files']:
                                        zf = zipfile.ZipFile(file_bytes)
                                        csv_files = zf.namelist()
                                        print('csv_files:-',csv_files)
                                        for one_file in csv_files:
                                            fil = one_file.replace('-_','').split('/')[-1].split('.')
                                            table_name = '_'.join(fil[0].split('_')[:-1:])#.replace('1','one').replace('2','two')
                                            print(table_name)
                                            one_file = zf.open(one_file)
                                            print('table_name',table_name)

                                            if len(table_name) is not 0:
                                                if upload_params.loc[self.param_ID,'Attachment File Name'] ==  'no attachment-Smytten':  #specific table name mapping for smytten
                                                   try:
                                                       table_name = smytten_table_mapping[table_name]
                                                       print('ingesting with this new  fileneame', table_name)
                                                   except:
                                                       print('no table mapping for',table_name)

                                                try:
                                                    df = pd.read_csv(one_file)
                                                except:
                                                    try:
                                                        df = pd.read_excel(one_file)
                                                    except:
                                                        continue

                                                df['updated_datetime'] = email_date
                                                df = df.astype(str)
                                                df = df.fillna('nan')
                                                # print(df.head(2))

                                                credentials = service_account.Credentials.from_service_account_file(
                                                    'Cred_files/' + upload_params.loc[
                                                        self.param_ID, 'Credential Json File'], )
                                                bq_client = bigquery.Client(credentials=credentials,
                                                                            project=upload_params.loc[
                                                                                self.param_ID, 'Project ID'])

                                                try:
                                                    df.drop(columns=["Unnamed: 0"], axis=1, inplace=True)
                                                except:
                                                    pass

                                                col_rename = {}
                                                col_mes = []
                                                for col in df.columns:
                                                    if 'Message' in col:
                                                        col_mes.append(col.strip())
                                                    elif 'Message' not in col:
                                                        col_new = column_name_formatting(col)
                                                        col_rename[col] = col_new.strip()

                                                try:
                                                    df.drop(columns=col_mes, axis=1, inplace=True)
                                                except:
                                                    pass
                                                df.rename(columns=col_rename, inplace=True)
                                                # print(col_mes)

                                                df = df.astype(str)
                                                df = df.fillna('nan')
                                                print(df.columns)
                                                print(df.info(verbose=True))
                                                print(df.head(2))
                                                # Uploading data to gbq
                                                try:
                                                    df.to_gbq(upload_params.loc[
                                                                  self.param_ID, 'Destination Table'] + '.' + table_name,
                                                              upload_params.loc[self.param_ID, 'Project ID'],
                                                              credentials=credentials,
                                                              if_exists=upload_params.loc[self.param_ID, 'IF Exists'])
                                                    print('ingested')
                                                    print(today_date)
                                                except:
                                                    stamp_date = str(today_date).replace('-', "_")
                                                    df.to_gbq(upload_params.loc[
                                                                  self.param_ID, 'Destination Table'] + '.' + table_name + stamp_date,
                                                              upload_params.loc[self.param_ID, 'Project ID'],
                                                              credentials=credentials,
                                                              if_exists=upload_params.loc[self.param_ID, 'IF Exists'])
                                                    send_email(
                                                        from_email=upload_params.loc[self.param_ID, 'Error_From_Email'],
                                                        # send an email if there is an error in uploading
                                                        from_email_pass=upload_params.loc[
                                                            self.param_ID, 'Error_From_Email_Password'],
                                                        to_email=upload_params.loc[self.param_ID, 'Error_To_Email'],
                                                        subject='Error in Uploading file(s)',
                                                        body_text=f'Data is ingested to new table {dest_table} : {e}')




                                    else:
                                        pass


                        except Exception as e:
                            traceback.print_exc()
                            logger.exception('ERROR')
                            print(logger.exception('Error'))
                            dest_table = upload_params.loc[self.param_ID,'Destination Table']
                            send_email(from_email=upload_params.loc[self.param_ID,'Error_From_Email'],                   #send an email if there is an error in uploading
                                        from_email_pass=upload_params.loc[self.param_ID,'Error_From_Email_Password'],
                                        to_email=upload_params.loc[self.param_ID,'Error_To_Email'],
                                        subject='Error in Uploading file(s)',
                                        body_text=f'There was an error in uploading the file to {dest_table} : {e}')
                            print('--Error--\n')
                            print(e)



if __name__ == '__main__':
    logger.info('Gmail to BigQuery Common Script has started')
    param_id_list = upload_params.index.to_list()
    param_id_list = [5,10,11] #[5]
    for param_ID in param_id_list:
        #date = (datetime.today().date() - timedelta(days = 2)).strftime('%d-%b-%Y')
        #print(date)
        
        last_run_date = (upload_params.loc[param_ID, 'Last Run Date'].date() - timedelta(days = 0)).strftime('%d-%b-%Y')    #last run date
        today_date = datetime.today().date()                                #today's date
        hour_now = datetime.now(pytz.timezone('Asia/Kolkata')).time().hour      #time now (hour)
        print("Today's date:",today_date, "\nLast Run date:",upload_params.loc[param_ID, 'Last Run Date'].date())
        print("param_ID:-",param_ID)
        # For Daily Uploading or uploading with fixed day difference 
        print("upload_params.loc[param_ID, 'Time to Run']:-",upload_params.loc[param_ID, 'Time to Run'])
        if upload_params.loc[param_ID, 'Run Type'] == 'Daily':
            run_gap = int(upload_params.loc[param_ID, 'Run Gap'])
            print('upload_params',upload_params)
             
            if (upload_params.loc[param_ID, 'Last Run Date'].date() + timedelta(days = run_gap) <= today_date) & (upload_params.loc[param_ID, 'Time to Run'] <= hour_now) & (upload_params.loc[param_ID, 'Report Name'] != 'test'):
                print('Running...')
                obj = Email_To_BQ(last_run_date,param_ID=param_ID)
                #logger.info(f'Running for {upload_params.iloc[param_ID]["Report Name"]}')
                #obj.run()
                #upload_params.loc[param_ID, 'Last Run Date'] = today_date.strftime('%Y-%m-%d')      #updating last run date
            #elif (param_ID == 4) & (upload_params.loc[param_ID, 'Time to Run'] <= hour_now):
            elif  upload_params.loc[param_ID, 'Report Name'] == 'test':
                print('insidee..')
                obj = Email_To_BQ(last_run_date,param_ID=param_ID)
                obj.run()

        '''
        # For Hourly Uploading or uploading with fixed hour difference
        elif upload_params.loc[param_ID, 'Run Type'] == 'Hourly':
            run_gap = int(upload_params.loc[param_ID, 'Run Gap'])

            if upload_params.loc[param_ID, 'Last Run Date'].date() < today_date:
                print('Running...')                
                obj = Email_To_BQ(last_run_date,param_ID=param_ID)
                logger.info(f'Running for {upload_params.iloc[param_ID]["Report Name"]}')
                obj.run()

            elif (upload_params.loc[param_ID, 'Last Run Date'].date() == today_date) & (int(upload_params.loc[param_ID, 'Last Run Hour']) + int(run_gap) <= hour_now):
                print('Running...')
                obj = Email_To_BQ(last_run_date,param_ID=param_ID)
                logger.info(f'Running for {upload_params.iloc[param_ID]["Report Name"]}')
                obj.run()
        '''


    # Updated Param Sheet
    #upload_params['Last Run Date'] = upload_params['Last Run Date'].astype(str)
    #upload_params = upload_params.reset_index()
    #uploadparms_sheet.update([upload_params.columns.values.tolist()] + upload_params.values.tolist())

    #logger.info('Gmail to BigQuery Common Script has Stopped')
