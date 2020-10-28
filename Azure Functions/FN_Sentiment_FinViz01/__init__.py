import logging
import yahoo_fin.stock_info as si
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import talib
import requests
import json
import openpyxl
import psycopg2 as pg

import azure.functions as func

#db_str = os.environ.get("AZ_DB_Key")
#finn_token = os.environ.get("Finnhub_Key")
db_str = "dbname=StonksGoUp user=postgres host=localhost password=admin"

low = float(2.5)
high = float(25.0)
to = int(datetime.strptime(datetime.today().strftime("%d/%m/%Y") + " +0000", "%d/%m/%Y %z").timestamp())
fro = int((datetime.strptime(datetime.today().strftime("%d/%m/%Y") + " +0000", "%d/%m/%Y %z")-relativedelta(days=300)).timestamp())
earnings_period = int((datetime.strptime(datetime.today().strftime("%d/%m/%Y") + " +0000", "%d/%m/%Y %z")+relativedelta(days=5)).timestamp())
capital = 100000
risk = 0.05

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    conn, cur = get_connected(db_str)
    tickers = get_tickers(cur)

    print(tickers)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    if 'cur' in locals():
                cur.close()
                conn.close() 
    return None

def get_connected(db_str):
    conn = pg.connect(db_str)
    cur = conn.cursor()

    return conn, cur

def get_tickers(cur):
    SQL_tickers = """SELECT ticker FROM tickers ORDER BY ticker ASC"""
    cur.execute(SQL_tickers)

    tickers = list([i[0] for i in cur.fetchall()])

    return tickers