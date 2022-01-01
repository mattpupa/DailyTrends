
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
    # https://googleapis.dev/python/bigquery/latest/usage/client.html
    bq_client = bigquery.Client()

    """
    1. Subquery is used to find the latest available week in the table
    2. Query finds the top terms of the week, as of yesterday in Los Angelas that are in the top 10 (by rank)
    3. Query is joined on subquery to make sure data is only pulling from latest available week
    """
    query_latest =  """
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

    query_job_latest = bq_client.query(query_latest) # Connect the SQL query to bigquery

    # https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
    results_latest = query_job_latest.result().to_dataframe() # save results to a dataframe

    query_previous =  """
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

    query_job_previous = bq_client.query(query_previous) # Connect the SQL query to bigquery

    # https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
    results_previous = query_job_previous.result().to_dataframe() # save results to a dataframe

    # Both dataframes are combined and duplicates are removed
    df_new_terms = results_latest.merge(results_previous.drop_duplicates(), on=['term'],
                       how='left', indicator=True)
    df_new_terms = df_new_terms.loc[df_new_terms['_merge'] == 'left_only']
    df_new_terms.drop(['_merge'], axis=1)

    # New top terms are saved as a list of lists, which is needed to write to google sheets
    new_terms = list(df_new_terms['term'])
    new_terms_list_of_lists = list(map(lambda t:[t], new_terms))



    #### Delete Current Data in Google Sheets via service account ####
    # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build('sheets', 'v4', credentials=credentials)

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = '1FvMRGaZ7Xk0M2HlICE3lrEzwBbpHHMdsXQYjqd21SWQ'
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


    
