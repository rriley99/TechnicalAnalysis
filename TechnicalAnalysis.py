import yahoo_fin.stock_info as si
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import talib
import requests
import json
import openpyxl
import psycopg2 as pg

class TechnicalAnalysis:
    
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur
    
    def update_data(self, conn, cur):

        self.update_tickers = """SELECT ticker, extract(epoch from MAX(quotedate)) as from, extract(epoch from now())::integer as to
                                    FROM stockdata 
                                    GROUP BY ticker
                                    HAVING MAX(quotedate) <> CURRENT_DATE"""
        self.cur.execute(self.update_tickers, self.conn)
        
        self.update_list = self.cur.fetchall()
        test = update_list[0]

        for self.ticker in update_list:
            r = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={test[0]}&resolution=D&from={test[1]}&to={test[2]}&token={finn_token}')
            data = pd.DataFrame(r.json())

            del data['s']
            data.columns = ['close', 'high', 'low', 'open', 'quotedate', 'volume']
            data['ticker'] = test[0]

            data['quotedate'] = data['quotedate'].apply(lambda x: datetime.fromtimestamp(x).date())
            print(data)

        # insert = [list(row) for row in data.itertuples(index=False)]
        # sql_insert = """INSERT INTO public.stockdata(close, high, low, open, quotedate, volume, ticker)
        #                 VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""
        # cur.executemany(sql_insert, insert)
        # conn.commit()
                    
        return self.update_list