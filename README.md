# DailyTrends

An automation hosted on google cloud with cloud functions and cloud scheduler. Once a day, 2 queries are run on a 'top trends' table in google bigquery. The 2 queries look at the top trends from the last 2 days, and only saves trends that are new.

Those trends are then sent to google drive and written into an existing google sheet. The data is refreshed each day.
