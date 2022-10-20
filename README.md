# Gmail to BQ (General)

Exporting data directly from email to google bigquery table.
This solution enables users to regularly upload data from email by searching email inbox directly to BigQuery table.

The functional steps in this process are broadly listed below:

- **Searching Inbox :** Searching for a desired email(s) based on a search query. Enlisting all the emails that qualify and then look for attachements if any.
- **Extracting data from Attachemtns and reading it :** This is done while also checking if the file is already ingested in to the table by referring ingested_files logs. If the file is already successfully ingested then nothing is done.
- **Transforming and uploading to respective Tables in gbq :**  Columns are filtered, necessary modifications are made to the column names and the data is uploaded to gbq table.
- **Updated Datetime:** A column with the Timestamp value of the DateTime of email is added to the data(corresponding to timezone = 'Asia/Kolkata').
- **Cloud Scheduler :** A Cloud Scheduler job is used to schedule run the script periodically to upload data as it comes.


This process makes us of a configuratoin file, which is a google sheet, uploadparams.gsheet which is accessed and modified using Google Sheets API.
[link to g-sheet](https://docs.google.com/spreadsheets/d/1U4MekkmAesryDVbGrvBl07nEZOEQSxugylmZPJAEiN0/)

The description of parameters are enlisted in the second sheet (info).
