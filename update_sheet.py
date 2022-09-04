from __future__ import print_function

from hattricklib import connect_hattrick, download_data
from datetime import datetime
import os.path
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '15EI8EnoqGo36QTRY7NMhWtpgrWJsI1GsyLHcdJMKJcI'
SAMPLE_RANGE_NAME = 'Sheet1!E1:E2'


def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    if datetime.now().isoweekday() == 6:
        script_directory = os.path.dirname(os.path.abspath(__file__))


        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(f'{script_directory}/token.json'):
            creds = Credentials.from_authorized_user_file(f'{script_directory}/token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    f'{script_directory}/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(f'{script_directory}/token.json', 'w') as token:
                token.write(creds.to_json())

        print(f'{datetime.now().strftime("%H:%M:%S")} - Google sheet connection has been established.')

        try:
            service = build('sheets', 'v4', credentials=creds)

            spreadsheet_id = '15EI8EnoqGo36QTRY7NMhWtpgrWJsI1GsyLHcdJMKJcI'
            range_name = 'A1:DD20000'
            value_input_option = "USER_ENTERED"

            #download data from HT
            ht_oauth, ht_token = connect_hattrick()
            df_members_achievs = download_data(oauth=ht_oauth, token=ht_token)
            #to load index as well
            df_members_achievs = pd.read_excel(f'{script_directory}/Data/fed_achievments_current.xlsx')
            df_members_achievs.signup_date.fillna('', inplace=True)


            data = df_members_achievs.values.tolist()
            data.insert(0, list(df_members_achievs.columns.values))
            body = {
                'values': data
            }
            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_input_option, body=body).execute()

            print(f'{datetime.now().strftime("%H:%M:%S")} - Google sheet has been succesfully updated.')
                

        except HttpError as err:
            print(err)
    else:
        print('Scripts canceled. Today it\'s not Sunday.')

if __name__ == '__main__':
    main()