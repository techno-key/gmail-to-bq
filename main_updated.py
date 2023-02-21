    # Importing Libraries

import gspread
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import os

curr_dir = os.getcwd()
path = curr_dir + '/Cred_files' +'/' + 'smytten.json'
print(path)

credentials = service_account.Credentials.from_service_account_file('Cred_files/smytten.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = curr_dir + "/Cred_files/smytten.json"
client = bigquery.Client(credentials=credentials)



######################################## Active Base ######################################

## Email Function

def Active_Base_Email_Notification(data):
    email_df = data[['Date','Campaign_Name','Total_Sent','Total_Open','Total_clicks','Goal_1_Click_Through_Converted_Users']]
    email_df.rename(columns={'Campaign_Name':'Campaign_Name','Total_Sent':'Total_Sent','Total_Open':'Total_Open','Total_clicks':'Total_clicks',
                            'Goal_1_Click_Through_Converted_Users':'CTCR'},inplace=True)
    email_df[['Type', 'WeekCount']] = email_df["Campaign_Name"].apply(lambda x: pd.Series(str(x).split(":")))
    email_df[['Week', 'Day']] = email_df["WeekCount"].apply(lambda x: pd.Series(str(x).split("&")))
    email_df=email_df[['Date','Week','Day','Total_Sent','Total_Open','Total_clicks','CTCR']]
    email_df = email_df.sort_values(by = ['Date','Week','Day']).reset_index(drop=True)
    email_df['Day'] = email_df['Day'].apply(lambda x: x.replace(' ',''))
    df = email_df[['Date','Week','Day','Total_Sent']]
    df1 = email_df[['Date','Week','Day','Total_Open']]
    df2 = email_df[['Date','Week','Day','Total_clicks']]
    df3 = email_df[['Date','Week','Day','CTCR']]
    df_pivot = df.pivot_table(values=['Total_Sent'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df1_pivot = df1.pivot_table(values=['Total_Open'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df2_pivot = df2.pivot_table(values=['Total_clicks'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df3_pivot = df3.pivot_table(values=['CTCR'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df_pivot.columns = df_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df_pivot_df = df_pivot.sort_index(1).reset_index()

    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()

    df2_pivot.columns = df2_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df2_pivot_df = df2_pivot.sort_index(1).reset_index()

    df3_pivot.columns = df3_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df3_pivot_df = df3_pivot.sort_index(1).reset_index()
    df_pivot_df = df_pivot_df.fillna(0)
    df1_pivot_df = df1_pivot_df.fillna(0)
    df2_pivot_df = df2_pivot_df.fillna(0)
    df3_pivot_df = df3_pivot_df.fillna(0)
    df_pivot_df.insert(2,'Sent_Total',df_pivot_df.sum(axis=1 , numeric_only=True))
    df1_pivot_df.insert(2,'Open_Total',df1_pivot_df.sum(axis=1 , numeric_only=True))
    df2_pivot_df.insert(2,'Clicks_Total',df2_pivot_df.sum(axis=1 , numeric_only=True))
    df3_pivot_df.insert(2,'CTR_Total',df3_pivot_df.sum(axis=1 , numeric_only=True))
    Email_df = df_pivot_df.merge(df1_pivot_df,right_on=['Date','Week'],left_on=['Date','Week'])
    Email_df = Email_df.merge(df2_pivot_df,right_on=['Date','Week'],left_on=['Date','Week'])
    Email_df = Email_df.merge(df3_pivot_df,right_on=['Date','Week'],left_on=['Date','Week'])

    Email_df.columns = Email_df.columns.str.replace(": ", "_")
    Email_df.columns = Email_df.columns.str.replace(":", "_")
    Email_df.columns = Email_df.columns.str.replace("-", "_")
    Email_df.columns = Email_df.columns.str.replace(" ", "_")

    Email_df.loc[:, Email_df.columns != 'Date'] = Email_df.loc[:, Email_df.columns != 'Date'].astype('str')
    Email_df['Date'] = pd.to_datetime(Email_df['Date'])
    
    return Email_df    

## PUSH Function

def Active_Base_Push_Notification(data):
    df1 = data[['Date','Campaign_Name','All_Platform_Impressions','All_Platform_Clicks','Goal_1_Click_Through_Converted_Users_All_Platform']]
    df = df1.copy()
    df.rename(columns={'Goal_1_Click_Through_Converted_Users_All_Platform':'CTR'},inplace=True)
    df[['Day', 'WeekCount']] = df["Campaign_Name"].apply(lambda x: pd.Series(str(x).split(":")))
    df[['Week', 'Day']] = df["WeekCount"].apply(lambda x: pd.Series(str(x).split("&")))
    df = df[['Date','Week','Day','All_Platform_Impressions','All_Platform_Clicks','CTR']]
    df = df.sort_values(by = ['Date','Week','Day']).reset_index(drop=True)
    df['Day'] = df['Day'].apply(lambda x: x.replace(' ',''))
    df1 = df[['Date','Week','Day','All_Platform_Impressions']]
    df2 = df[['Date','Week','Day','All_Platform_Clicks']]
    df3 = df[['Date','Week','Day','CTR']]
    df1_pivot = df1.pivot_table(values=['All_Platform_Impressions'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df2_pivot = df2.pivot_table(values=['All_Platform_Clicks'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df3_pivot = df3.pivot_table(values=['CTR'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()

    df2_pivot.columns = df2_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df2_pivot_df = df2_pivot.sort_index(1).reset_index()

    df3_pivot.columns = df3_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df3_pivot_df = df3_pivot.sort_index(1).reset_index()
    
    df1_pivot_df = df1_pivot_df.fillna(0)
    df2_pivot_df = df2_pivot_df.fillna(0)
    df3_pivot_df = df3_pivot_df.fillna(0)
    
    df1_pivot_df.insert(2,'Total_Impressions',df1_pivot_df.sum(axis=1 , numeric_only=True))
    df2_pivot_df.insert(2,'Total_Clicks',df2_pivot_df.sum(axis=1 , numeric_only=True))
    df3_pivot_df.insert(2,'Total_CTR',df3_pivot_df.sum(axis=1 , numeric_only=True))
    
    Push_df = df1_pivot_df.merge(df2_pivot_df,right_on=['Date','Week'],left_on=['Date','Week'])
    Push_df = Push_df.merge(df3_pivot_df,right_on=['Date','Week'],left_on=['Date','Week']) 

    Push_df.columns = Push_df.columns.str.replace(": ", "_")
    Push_df.columns = Push_df.columns.str.replace(":", "_")
    Push_df.columns = Push_df.columns.str.replace("-", "_")
    Push_df.columns = Push_df.columns.str.replace(" ", "_")

    Push_df.loc[:, Push_df.columns != 'Date'] = Push_df.loc[:, Push_df.columns != 'Date'].astype('str')
    Push_df['Date'] = pd.to_datetime(Push_df['Date'])
    
    
    return Push_df

## SMS Function

def Active_Base_SMS_Notification(data):
    sms_df = data[['Date','Campaign_Name','Sent','Goal_1_Conversions']]
    # sms_df.rename(columns={'Campaign Name':'Campaign_Name','Goal 1 Conversions':'Goal_1_Conversions'},inplace=True)
    sms_df[['Type', 'WeekCount']] = sms_df["Campaign_Name"].apply(lambda x: pd.Series(str(x).split(":")))
    sms_df[['Week', 'Day']] = sms_df["WeekCount"].apply(lambda x: pd.Series(str(x).split("&")))
    sms_df=sms_df[['Date','Week','Day','Sent','Goal_1_Conversions']]
    sms_df = sms_df.sort_values(by = ['Date','Week','Day']).reset_index(drop=True)
    sms_df['Day'] = sms_df['Day'].apply(lambda x: x.replace(' ',''))

    df = sms_df[['Date','Week','Day','Sent']]
    df1 = sms_df[['Date','Week','Day','Goal_1_Conversions']]

    df_pivot = df.pivot_table(values=['Sent'],index=['Date','Week'],columns='Day',aggfunc='sum')
    df1_pivot = df1.pivot_table(values=['Goal_1_Conversions'],index=['Date','Week'],columns='Day',aggfunc='sum')

    df_pivot.columns = df_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df_pivot_df = df_pivot.sort_index(1).reset_index()

    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()

    df_pivot_df = df_pivot_df.fillna(0)
    df1_pivot_df = df1_pivot_df.fillna(0)

    df_pivot_df.insert(2,'Total_Sent',df_pivot_df.sum(axis=1 , numeric_only=True))
    df1_pivot_df.insert(2,'Total_Goal_1_Conversions',df1_pivot_df.sum(axis=1 , numeric_only=True))

    SMS_df = df_pivot_df.merge(df1_pivot_df,right_on=['Date','Week'],left_on=['Date','Week'])

    SMS_df.columns = SMS_df.columns.str.replace(": ", "_")
    SMS_df.columns = SMS_df.columns.str.replace(":", "_")
    SMS_df.columns = SMS_df.columns.str.replace("-", "_")
    SMS_df.columns = SMS_df.columns.str.replace(" ", "_")
    
    SMS_df.loc[:, SMS_df.columns != 'Date'] = SMS_df.loc[:, SMS_df.columns != 'Date'].astype('str')
    SMS_df['Date'] = pd.to_datetime(SMS_df['Date'])
    
    return SMS_df

## Data Extraction From Big Query

Active_Base_Onetime_Email_query = '''
SELECT * FROM `smytten-374507.Prepped_Table.Active_Base_Onetime_Email`
'''
result = client.query(Active_Base_Onetime_Email_query)
Active_Base_email_df = result.to_dataframe()

Active_Base_Onetime_Push_query = '''
SELECT * FROM `smytten-374507.Prepped_Table.Active_Base_Onetime_Push` 
'''
result = client.query(Active_Base_Onetime_Push_query)
Active_Base_Push_df = result.to_dataframe()

Active_Base_Onetime_SMS_query = '''
SELECT * FROM `smytten-374507.Prepped_Table.Active_Base_Onetime_SMS` 
'''
result = client.query(Active_Base_Onetime_SMS_query)
Active_Base_SMS_df = result.to_dataframe()

## Function Call
Active_Base_email_df = Active_Base_Email_Notification(Active_Base_email_df)
Active_Base_Push_df = Active_Base_Push_Notification(Active_Base_Push_df)
Active_Base_SMS_df = Active_Base_SMS_Notification(Active_Base_SMS_df)

## Data Frame To Google Sheet 

# Google sheet Json read
gc = gspread.service_account(curr_dir + '/Cred_files/sheet_1.json')
Active_Base_sh = gc.open_by_key('1VHPHx3H5Cxp53QhDerH0wwVIuLaUeExsQfkMb7W79yk')

# # Active Base One Time Data append
# # EMAIL
Email_worksheet = Active_Base_sh.worksheet('Email')
Email_worksheet.clear()
Active_Base_email_df['Date'] = Active_Base_email_df['Date'].astype('str')
Email_worksheet.update([Active_Base_email_df.columns.values.tolist()] + Active_Base_email_df.values.tolist())

# Push
Push_worksheet = Active_Base_sh.worksheet('Push')
Push_worksheet.clear()
Active_Base_Push_df['Date'] = Active_Base_Push_df['Date'].astype('str')
Push_worksheet.update([Active_Base_Push_df.columns.values.tolist()] + Active_Base_Push_df.values.tolist())

# # SMS
SMS_worksheet = Active_Base_sh.worksheet('SMS')
SMS_worksheet.clear()
Active_Base_SMS_df['Date'] = Active_Base_SMS_df['Date'].astype('str')
SMS_worksheet.update([Active_Base_SMS_df.columns.values.tolist()] + Active_Base_SMS_df.values.tolist())

################################## Segments #######################################################

## EMail Function

def Email(data):
    email_df = data[['Date','Campaign_Name','Total_Sent','Total_Open','Total_clicks','Goal_1_Click_Through_Converted_Users']]
    email_df.rename(columns={'Goal_1_Click_Through_Converted_Users':'CTCR'},inplace=True)
    df_pivot = email_df.pivot_table(values=['Total_Sent'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df1_pivot = email_df.pivot_table(values=['Total_Open'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df2_pivot = email_df.pivot_table(values=['Total_clicks'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df3_pivot = email_df.pivot_table(values=['CTCR'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df_pivot.columns = df_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df_pivot_df = df_pivot.sort_index(1).reset_index()

    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()

    df2_pivot.columns = df2_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df2_pivot_df = df2_pivot.sort_index(1).reset_index()

    df3_pivot.columns = df3_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df3_pivot_df = df3_pivot.sort_index(1).reset_index()
    
    df_pivot_df.insert(1,'Sent_Total',df_pivot_df.sum(axis=1 , numeric_only=True))
    df1_pivot_df.insert(1,'Open_Total',df1_pivot_df.sum(axis=1 , numeric_only=True))
    df2_pivot_df.insert(1,'Clicks_Total',df2_pivot_df.sum(axis=1 , numeric_only=True))
    df3_pivot_df.insert(1,'CTR_Total',df3_pivot_df.sum(axis=1 , numeric_only=True))
    
    Email_df = df_pivot_df.merge(df1_pivot_df,right_on=['Date'],left_on=['Date'])
    Email_df = Email_df.merge(df2_pivot_df,right_on=['Date'],left_on=['Date'])
    Email_df = Email_df.merge(df3_pivot_df,right_on=['Date'],left_on=['Date'])

    Email_df.columns = Email_df.columns.str.replace(": ", "_")
    Email_df.columns = Email_df.columns.str.replace(":", "_")
    Email_df.columns = Email_df.columns.str.replace("-", "_")
    Email_df.columns = Email_df.columns.str.replace(" ", "_")

    Email_df.loc[:, Email_df.columns != 'Date'] = Email_df.loc[:, Email_df.columns != 'Date'].astype('str')
    Email_df['Date'] = pd.to_datetime(Email_df['Date'])
    
    return Email_df

## Push Function

def Push(data):
    push_df = data[['Date','Campaign_Name','All_Platform_Impressions','All_Platform_Clicks','Goal_1_Click_Through_Converted_Users_All_Platform']]
    push_df.rename(columns={'Goal_1_Click_Through_Converted_Users_All_Platform':'CTR'},inplace=True)
    df_pivot = push_df.pivot_table(values=['All_Platform_Impressions'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df1_pivot = push_df.pivot_table(values=['All_Platform_Clicks'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df2_pivot = push_df.pivot_table(values=['CTR'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    
    df_pivot.columns = df_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df_pivot_df = df_pivot.sort_index(1).reset_index()

    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()

    df2_pivot.columns = df2_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df2_pivot_df = df2_pivot.sort_index(1).reset_index()
    
    df_pivot_df.insert(1,'Total_Impressions',df_pivot_df.sum(axis=1 , numeric_only=True))
    df1_pivot_df.insert(1,'Total_Clicks',df1_pivot_df.sum(axis=1 , numeric_only=True))
    df2_pivot_df.insert(1,'Total_CTR',df2_pivot_df.sum(axis=1 , numeric_only=True))
    
    push_df = df_pivot_df.merge(df1_pivot_df,right_on=['Date'],left_on=['Date'])
    push_df = push_df.merge(df2_pivot_df,right_on=['Date'],left_on=['Date'])

    push_df.columns = push_df.columns.str.replace(": ", "_")
    push_df.columns = push_df.columns.str.replace(":", "_")
    push_df.columns = push_df.columns.str.replace("-", "_")
    push_df.columns = push_df.columns.str.replace(" ", "_")

    push_df.loc[:, push_df.columns != 'Date'] = push_df.loc[:, push_df.columns != 'Date'].astype('str')
    push_df['Date'] = pd.to_datetime(push_df['Date'])
    
    return  push_df

## SMS Function

def SMS(data):
    sms_df = data[['Date','Campaign_Name','Sent','Goal_1_Conversions']]
    # sms_df.rename(columns={'Campaign Name':'Campaign_Name','Goal 1 Conversions':'Goal_1_Conversions'},inplace=True)
    df_pivot = sms_df.pivot_table(values=['Sent'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    df1_pivot = sms_df.pivot_table(values=['Goal_1_Conversions'],index=['Date'],columns='Campaign_Name',aggfunc='sum')
    
    df_pivot.columns = df_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df_pivot_df = df_pivot.sort_index(1).reset_index()

    df1_pivot.columns = df1_pivot.columns.map('{0[0]}_{0[1]}'.format)
    df1_pivot_df = df1_pivot.sort_index(1).reset_index()
    
    df_pivot_df.insert(1,'Total_Sent',df_pivot_df.sum(axis=1 , numeric_only=True))
    df1_pivot_df.insert(1,'Total_Conversions',df1_pivot_df.sum(axis=1 , numeric_only=True))
    
    sms_df = df_pivot_df.merge(df1_pivot_df,right_on=['Date'],left_on=['Date'])

    sms_df.columns = sms_df.columns.str.replace(": ", "_")   
    sms_df.columns = sms_df.columns.str.replace(":", "_") 
    sms_df.columns = sms_df.columns.str.replace("-", "_")
    sms_df.columns = sms_df.columns.str.replace(" ", "_")  

    sms_df.loc[:, sms_df.columns != 'Date'] = sms_df.loc[:, sms_df.columns != 'Date'].astype('str')
    sms_df['Date'] = pd.to_datetime(sms_df['Date'])
    
    
    return  sms_df

###################### Segment 3 Daily_Onboarding_Checkout_Drop_Flow ##################################

## Data Extraction From Big Query

Daily_Onboarding_Checkout_Drop_Flow_Email = '''
SELECT * FROM `smytten-374507.Prepped_Table.Daily_Onboarding_Checkout_Drop_Flow_Email`
'''
result = client.query(Daily_Onboarding_Checkout_Drop_Flow_Email)
Daily_Onboarding_Checkout_Drop_Email_df = result.to_dataframe()

Daily_Onboarding_Checkout_Drop_Flow_Push = '''
SELECT * FROM `smytten-374507.Prepped_Table.Daily_Onboarding_Checkout_Drop_Flow_Push`
'''
result = client.query(Daily_Onboarding_Checkout_Drop_Flow_Push)
Daily_Onboarding_Checkout_Drop_Push_df = result.to_dataframe()

Daily_Onboarding_Checkout_Drop_Flow_SMS = '''
SELECT * FROM `smytten-374507.Prepped_Table.Daily_Onboarding_Checkout_Drop_Flow_SMS` 
'''
result = client.query(Daily_Onboarding_Checkout_Drop_Flow_SMS)
Daily_Onboarding_Checkout_Drop_SMS_df = result.to_dataframe()


Daily_Onboarding_Checkout_Drop_Email_df = Email(Daily_Onboarding_Checkout_Drop_Email_df)
Daily_Onboarding_Checkout_Drop_Push_df = Push(Daily_Onboarding_Checkout_Drop_Push_df)
Daily_Onboarding_Checkout_Drop_SMS_df = SMS(Daily_Onboarding_Checkout_Drop_SMS_df)

##### To Gsheet

segment3_Daily_Onboarding_Checkout_sh = gc.open_by_key('1uw21LzOzdtxqwRY54bP10kEBvhta33fLeNgZLRXdWUc')

Email_worksheet = segment3_Daily_Onboarding_Checkout_sh.worksheet('Email')
Email_worksheet.clear()
Daily_Onboarding_Checkout_Drop_Email_df['Date'] = Daily_Onboarding_Checkout_Drop_Email_df['Date'].astype('str')
Email_worksheet.update([Daily_Onboarding_Checkout_Drop_Email_df.columns.values.tolist()] + Daily_Onboarding_Checkout_Drop_Email_df.values.tolist())

# Push
Push_worksheet = segment3_Daily_Onboarding_Checkout_sh.worksheet('Push')
Push_worksheet.clear()
Daily_Onboarding_Checkout_Drop_Push_df['Date'] = Daily_Onboarding_Checkout_Drop_Push_df['Date'].astype('str')
Push_worksheet.update([Daily_Onboarding_Checkout_Drop_Push_df.columns.values.tolist()] + Daily_Onboarding_Checkout_Drop_Push_df.values.tolist())

# # SMS
SMS_worksheet = segment3_Daily_Onboarding_Checkout_sh.worksheet('SMS')
SMS_worksheet.clear()
Daily_Onboarding_Checkout_Drop_SMS_df['Date'] = Daily_Onboarding_Checkout_Drop_SMS_df['Date'].astype('str')
SMS_worksheet.update([Daily_Onboarding_Checkout_Drop_SMS_df.columns.values.tolist()] + Daily_Onboarding_Checkout_Drop_SMS_df.values.tolist())


################################### Segment 1 Reg TO ATC ##############################################

## Data Extraction from Bigquery

Segment_1_Reg_to_ATC_Email = '''
SELECT * FROM `smytten-374507.Prepped_Table.Subscribe_Segment_One_Reg_to_ATC_Email`
'''
result = client.query(Segment_1_Reg_to_ATC_Email)
Segment_1_Reg_to_ATC_Email = result.to_dataframe()

Segment_1_Reg_to_ATC_Push = '''
SELECT * FROM `smytten-374507.Prepped_Table.Subscribe_Segment_One_Reg_to_ATC_Push`
'''
result = client.query(Segment_1_Reg_to_ATC_Push)
Segment_1_Reg_to_ATC_Push = result.to_dataframe()

# Segment_1_Reg_to_ATC_SMS = '''
# SELECT * FROM `smytten-374507.Prepped_Table.Daily_Onboarding_Checkout_Drop_Flow_SMS` 
# '''
# result = client.query(Daily_Onboarding_Checkout_Drop_Flow_SMS)
# Daily_Onboarding_Checkout_Drop_SMS_df = result.to_dataframe()


Segment_1_Reg_to_ATC_Email = Email(Segment_1_Reg_to_ATC_Email)
Segment_1_Reg_to_ATC_Push = Push(Segment_1_Reg_to_ATC_Push)
# Daily_Onboarding_Checkout_Drop_SMS_df = SMS(Daily_Onboarding_Checkout_Drop_SMS_df)

## To GSheet

Reg_To_ATC_sh = gc.open_by_key('1605CtR0zLccc-yzRUsgV9KrDcC-dpjFKIu6FqNs_sXA')

Email_worksheet = Reg_To_ATC_sh.worksheet('Email')
Email_worksheet.clear()
Segment_1_Reg_to_ATC_Email['Date'] = Segment_1_Reg_to_ATC_Email['Date'].astype('str')
Email_worksheet.update([Segment_1_Reg_to_ATC_Email.columns.values.tolist()] + Segment_1_Reg_to_ATC_Email.values.tolist())

# Push
Push_worksheet = Reg_To_ATC_sh.worksheet('Push')
Push_worksheet.clear()
Segment_1_Reg_to_ATC_Push['Date'] = Segment_1_Reg_to_ATC_Push['Date'].astype('str')
Push_worksheet.update([Segment_1_Reg_to_ATC_Push.columns.values.tolist()] + Segment_1_Reg_to_ATC_Push.values.tolist())

# # # SMS
# SMS_worksheet = Reg_To_ATC_sh.worksheet('SMS')
# SMS_worksheet.clear()
# Daily_Onboarding_Checkout_Drop_SMS_df['Date'] = Daily_Onboarding_Checkout_Drop_SMS_df['Date'].astype('str')
# SMS_worksheet.update([Daily_Onboarding_Checkout_Drop_SMS_df.columns.values.tolist()] + Daily_Onboarding_Checkout_Drop_SMS_df.values.tolist())

################################### Segment 2 ATC To Checkout ##############################################

## Data Extraction from Bigquery

Segment_2_ATC_to_Checkout_Email = '''
SELECT * FROM `smytten-374507.Prepped_Table.Subscribe_Segment_Two_ATC_to_Checkout_Email`
'''
result = client.query(Segment_2_ATC_to_Checkout_Email)
Segment_2_ATC_to_Checkout_Email = result.to_dataframe()

Segment_2_ATC_to_Checkout_Push = '''
SELECT * FROM `smytten-374507.Prepped_Table.Subscribe_Segment_Two_ATC_to_Checkout_Push`
'''
result = client.query(Segment_2_ATC_to_Checkout_Push)
Segment_2_ATC_to_Checkout_Push = result.to_dataframe()

Segment_2_ATC_to_Checkout_SMS = '''
SELECT * FROM `smytten-374507.Prepped_Table.Subscribe_Segment_Two_ATC_to_Checkout_SMS` 
'''
result = client.query(Segment_2_ATC_to_Checkout_SMS)
Segment_2_ATC_to_Checkout_SMS = result.to_dataframe()

Segment_2_ATC_to_Checkout_Email = Email(Segment_2_ATC_to_Checkout_Email)
Segment_2_ATC_to_Checkout_Push = Push(Segment_2_ATC_to_Checkout_Push)
Segment_2_ATC_to_Checkout_SMS = SMS(Segment_2_ATC_to_Checkout_SMS)

ATC_to_Checkout_sh = gc.open_by_key('1cGHcdGBCOyujmQVU3OfN9Btk1d-3BJMtoaBYmCDgp2E')


Email_worksheet = ATC_to_Checkout_sh.worksheet('Email')
Email_worksheet.clear()
Segment_2_ATC_to_Checkout_Email['Date'] = Segment_2_ATC_to_Checkout_Email['Date'].astype('str')
Email_worksheet.update([Segment_2_ATC_to_Checkout_Email.columns.values.tolist()] + Segment_2_ATC_to_Checkout_Email.values.tolist())

# Push
Push_worksheet = ATC_to_Checkout_sh.worksheet('Push')
Push_worksheet.clear()
Segment_2_ATC_to_Checkout_Push['Date'] = Segment_2_ATC_to_Checkout_Push['Date'].astype('str')
Push_worksheet.update([Segment_2_ATC_to_Checkout_Push.columns.values.tolist()] + Segment_2_ATC_to_Checkout_Push.values.tolist())

# # SMS
SMS_worksheet = ATC_to_Checkout_sh.worksheet('SMS')
SMS_worksheet.clear()
Segment_2_ATC_to_Checkout_SMS['Date'] = Segment_2_ATC_to_Checkout_SMS['Date'].astype('str')
SMS_worksheet.update([Segment_2_ATC_to_Checkout_SMS.columns.values.tolist()] + Segment_2_ATC_to_Checkout_SMS.values.tolist())


################################### Retention_ATC_Drop_Funnel ##########################################

## Data Extraction from Bigquery

Retention_ATC_Drop_Funnel_Email = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_ATC_Drop_Funnel_Email`
'''
result = client.query(Retention_ATC_Drop_Funnel_Email)
Retention_ATC_Drop_Funnel_Email = result.to_dataframe()

Retention_ATC_Drop_Funnel_Push = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_ATC_Drop_Funnel_Push`
'''
result = client.query(Retention_ATC_Drop_Funnel_Push)
Retention_ATC_Drop_Funnel_Push = result.to_dataframe()

Retention_ATC_Drop_Funnel_SMS = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_ATC_Drop_Funnel_SMS`
'''
result = client.query(Retention_ATC_Drop_Funnel_SMS)
Retention_ATC_Drop_Funnel_SMS = result.to_dataframe()

Retention_ATC_Drop_Funnel_Email = Email(Retention_ATC_Drop_Funnel_Email)
Retention_ATC_Drop_Funnel_Push = Push(Retention_ATC_Drop_Funnel_Push)
Retention_ATC_Drop_Funnel_SMS = SMS(Retention_ATC_Drop_Funnel_SMS)

## To G Sheet

Retention_ATC_Drop_Funnel_sh = gc.open_by_key('151S5etrQLC8ga-VWnvtPeJpg6Hy4tyw5O73sv5RPsfc')


Email_worksheet = Retention_ATC_Drop_Funnel_sh.worksheet('Email')
Email_worksheet.clear()
Retention_ATC_Drop_Funnel_Email['Date'] = Retention_ATC_Drop_Funnel_Email['Date'].astype('str')
Email_worksheet.update([Retention_ATC_Drop_Funnel_Email.columns.values.tolist()] + Retention_ATC_Drop_Funnel_Email.values.tolist())

# Push
Push_worksheet = Retention_ATC_Drop_Funnel_sh.worksheet('Push')
Push_worksheet.clear()
Retention_ATC_Drop_Funnel_Push['Date'] = Retention_ATC_Drop_Funnel_Push['Date'].astype('str')
Push_worksheet.update([Retention_ATC_Drop_Funnel_Push.columns.values.tolist()] + Retention_ATC_Drop_Funnel_Push.values.tolist())

# # SMS
SMS_worksheet = Retention_ATC_Drop_Funnel_sh.worksheet('SMS')
SMS_worksheet.clear()
Retention_ATC_Drop_Funnel_SMS['Date'] = Retention_ATC_Drop_Funnel_SMS['Date'].astype('str')
SMS_worksheet.update([Retention_ATC_Drop_Funnel_SMS.columns.values.tolist()] + Retention_ATC_Drop_Funnel_SMS.values.tolist())


################################### Retention_Checkout_Drop_Funnel ##########################################

## Data Extraction from Bigquery

Retention_Checkout_Drop_Funnel_Email = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_Checkout_Drop_Funnel_Email`
'''
result = client.query(Retention_Checkout_Drop_Funnel_Email)
Retention_Checkout_Drop_Funnel_Email = result.to_dataframe()

Retention_Checkout_Drop_Funnel_Push = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_Checkout_Drop_Funnel_Push`
'''
result = client.query(Retention_Checkout_Drop_Funnel_Push)
Retention_Checkout_Drop_Funnel_Push = result.to_dataframe()

Retention_Checkout_Drop_Funnel_SMS = '''
SELECT * FROM `smytten-374507.Prepped_Table.Retention_Checkout_Drop_Funnel_SMS`
'''
result = client.query(Retention_Checkout_Drop_Funnel_SMS)
Retention_Checkout_Drop_Funnel_SMS = result.to_dataframe()

Retention_Checkout_Drop_Funnel_Email = Email(Retention_Checkout_Drop_Funnel_Email)
Retention_Checkout_Drop_Funnel_Push = Push(Retention_Checkout_Drop_Funnel_Push)
Retention_Checkout_Drop_Funnel_SMS = SMS(Retention_Checkout_Drop_Funnel_SMS)

## To G Sheet

Retention_Checkout_Drop_Funnel_sh = gc.open_by_key('1Za4KaSWT75vVoIJ9S3nmMdRf7iWB9A6-w7hv9x_5Nm4')


Email_worksheet = Retention_Checkout_Drop_Funnel_sh.worksheet('Email')
Email_worksheet.clear()
Retention_Checkout_Drop_Funnel_Email['Date'] = Retention_Checkout_Drop_Funnel_Email['Date'].astype('str')
Email_worksheet.update([Retention_Checkout_Drop_Funnel_Email.columns.values.tolist()] + Retention_Checkout_Drop_Funnel_Email.values.tolist())

# Push
Push_worksheet = Retention_Checkout_Drop_Funnel_sh.worksheet('Push')
Push_worksheet.clear()
Retention_Checkout_Drop_Funnel_Push['Date'] = Retention_Checkout_Drop_Funnel_Push['Date'].astype('str')
Push_worksheet.update([Retention_Checkout_Drop_Funnel_Push.columns.values.tolist()] + Retention_Checkout_Drop_Funnel_Push.values.tolist())

# # SMS
SMS_worksheet = Retention_Checkout_Drop_Funnel_sh.worksheet('SMS')
SMS_worksheet.clear()
Retention_Checkout_Drop_Funnel_SMS['Date'] = Retention_Checkout_Drop_Funnel_SMS['Date'].astype('str')
SMS_worksheet.update([Retention_Checkout_Drop_Funnel_SMS.columns.values.tolist()] + Retention_Checkout_Drop_Funnel_SMS.values.tolist())


################################### Onboarding Install To Registration ##########################################

## Data Extraction from Bigquery

Onboarding_Install_to_reg_push = '''
SELECT * FROM `smytten-374507.Prepped_Table.onboarding_Install_to_reg_push`
'''
result = client.query(Onboarding_Install_to_reg_push)
Onboarding_Install_to_reg_push = result.to_dataframe()

Onboarding_Install_to_reg_push = Push(Onboarding_Install_to_reg_push)

# Push
Onboarding_Install_to_reg_sh = gc.open_by_key('1MZEs483pLkTpkE-vTzTFuD8clzmAcLvl1e9MTqvbHIU')


Push_worksheet = Onboarding_Install_to_reg_sh.worksheet('Push_Notification')
Push_worksheet.clear()
Onboarding_Install_to_reg_push['Date'] = Onboarding_Install_to_reg_push['Date'].astype('str')
Push_worksheet.update([Onboarding_Install_to_reg_push.columns.values.tolist()] + Onboarding_Install_to_reg_push.values.tolist())

#Segment 1
pandas_gbq.to_gbq(Segment_1_Reg_to_ATC_Email, "Analysis_Table.Segment_1_Reg_To_ATC_Email", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Segment_1_Reg_to_ATC_Push, "Analysis_Table.Segment_1_Reg_To_ATC_Push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

# #Segment 2
pandas_gbq.to_gbq(Segment_2_ATC_to_Checkout_Email, "Analysis_Table.Segment_2_ATC_To_Checkout_Email", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Segment_2_ATC_to_Checkout_Push, "Analysis_Table.Segment_2_ATC_To_Checkout_Push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Segment_2_ATC_to_Checkout_SMS, "Analysis_Table.Segment_2_ATC_To_Checkout_SMS", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

# #Segment 3
pandas_gbq.to_gbq(Daily_Onboarding_Checkout_Drop_Email_df, "Analysis_Table.Segment_3_Checkout_To_Onetime_Email", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Daily_Onboarding_Checkout_Drop_Push_df, "Analysis_Table.Segment_3_Checkout_To_Onetime_Push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Daily_Onboarding_Checkout_Drop_SMS_df, "Analysis_Table.Segment_3_Checkout_To_Onetime_SMS", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

# #Retention ATC
pandas_gbq.to_gbq(Retention_ATC_Drop_Funnel_Email, "Analysis_Table.Retention_ATC_Drop_Funnel_Email", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Retention_ATC_Drop_Funnel_Push, "Analysis_Table.Retention_ATC_Drop_Funnel_Push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Retention_ATC_Drop_Funnel_SMS, "Analysis_Table.Retention_ATC_Drop_Funnel_SMS", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

# #Retention Checkout
pandas_gbq.to_gbq(Retention_Checkout_Drop_Funnel_Email, "Analysis_Table.Retention_Checkout_Drop_Funnel_Email", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Retention_Checkout_Drop_Funnel_Push, "Analysis_Table.Retention_Checkout_Drop_Funnel_Push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

pandas_gbq.to_gbq(Retention_Checkout_Drop_Funnel_SMS, "Analysis_Table.Retention_Checkout_Drop_Funnel_SMS", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 

# #Onboarding Install to Reg
pandas_gbq.to_gbq(Onboarding_Install_to_reg_push, "Analysis_Table.Onboarding_Install_to_reg_push", project_id="smytten-374507", 
                  credentials=credentials, if_exists='replace') 





