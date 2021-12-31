
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import google.auth

def get_new_terms(request):
    # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.from_service_account_json
    bq_client = bigquery.Client()

    query_latest =  f"""
        select
            distinct term
        from (
            select
                max(week) as latest_week
            from `bigquery-public-data.google_trends.top_terms`
        ) max_week
        inner join `bigquery-public-data.google_trends.top_terms` tt
            on max_week.latest_week = tt.week
        where
            refresh_date = current_date - 1
            and dma_name = 'Los Angeles CA'
            and rank <= 10
    """

    query_job_latest = bq_client.query(query_latest) # Make the API request

    # https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
    results_latest = query_job_latest.result().to_dataframe()

    query_previous =  f"""
        select
            distinct term
        from (
            select
                max(week) as latest_week
            from `bigquery-public-data.google_trends.top_terms`
        ) max_week
        inner join `bigquery-public-data.google_trends.top_terms` tt
            on max_week.latest_week = tt.week
        where
            refresh_date = current_date - 2
            and dma_name = 'Los Angeles CA'
            and rank <= 10
    """

    query_job_previous = bq_client.query(query_previous) # Make the API request

    # https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
    results_previous = query_job_previous.result().to_dataframe()


    df_new_terms = results_latest.merge(results_previous.drop_duplicates(), on=['term'],
                       how='left', indicator=True)

    new_terms = list(df_new_terms['term'])

    new_terms_list_of_lists = list(map(lambda t:[t], new_terms))


    #### Delete Current Data in Google Sheets via service account ####
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build('sheets', 'v4', credentials=credentials)

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = 'thisisthespreadsheetID'
    RANGE = 'Terms!A2:A'

    # Call the Sheets API
    sheet = service.spreadsheets()

    request_delete = service.spreadsheets().values().batchClear(spreadsheetId=SPREADSHEET_ID,
                                                    body={'ranges': [RANGE]})
    response_delete = request_delete.execute()

    #### Send Data to Google Sheets via service account ####
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update


    request_write = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID,
                                                     range=RANGE,
                                                     valueInputOption='USER_ENTERED',
                                                     body={'values': new_terms_list_of_lists})
    response_write = request_write.execute()

    return "Automation finished!"


    
