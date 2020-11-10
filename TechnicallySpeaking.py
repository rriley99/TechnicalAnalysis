import psycopg2 as pg
from datetime import datetime, timedelta
import requests
import json
import numpy as np
import pandas as pd
from DataLoading import DataLoading
from TechnicalAnalysis import *
import time 

capital = 5000
risk = 0.0562
conn = pg.connect("dbname=StonksGoUp user=postgres host=localhost password=admin")
cur = conn.cursor()

with open('local_settings.txt') as f:
    json_local = json.load(f)

finn_token = json_local["finn_token"]

sql_tickers = """SELECT 
                ticker
                FROM tickers
                WHERE issp500='true'
                GROUP BY ticker
                ORDER BY ticker ASC
                LIMIT 25
                """
cur.execute(sql_tickers,conn)
tickers = cur.fetchall()
start_time = datetime.now()

df_analyzed = do_analysis(conn, cur, finn_token, tickers, capital, risk)
print(df_analyzed)

outputfile = f'Output_{datetime.now().date()}.xlsx'
df_analyzed.to_excel(outputfile)

end_time = datetime.now()
print(f"Completed in {end_time - start_time}")

if 'cur' in locals():
    cur.close()
    conn.close() 
