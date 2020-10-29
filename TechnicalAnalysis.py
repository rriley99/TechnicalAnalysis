import yahoo_fin.stock_info as si
import pandas as pd
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
import talib
import requests
import json
import openpyxl
import psycopg2 as pg
from Sentiment import Sentiment_FinViz

class TechnicalAnalysis:
    
    def __init__(self, conn, cur, finn_token, tickers, capital, risk):
        self.talib = talib
        self.conn = conn
        self.cur = cur
        self.finn_token = finn_token
        self.capital = capital
        self.risk = risk
        self.tickers = tickers
        self.df_analyzed = pd.DataFrame(columns=['Ticker', 'Open', 'Quote', 'RSI', 'Trend', 'Above200', 'Earnings', 'Supp/Res', 'S/R Price', 'Pullback'])
        
        
        # get earnings to avoid over clocking the API
        self.df_earnings = self.get_earnings(finn_token)
             
        for self.ticker in self.tickers:
            self.update_data(self.ticker, conn, cur, finn_token)
            
            print(f"Now querying {self.ticker[0]}")
            # Get historical data
            self.data = self.get_hist(self.ticker, conn)
    
            # Add indicator data
            self.indicated_data = self.get_indicators(self.data)
    
            # Analyze stonks:
            self.df_analyzed = self.analyze_chart(self.ticker, self.indicated_data, self.df_analyzed, self.df_earnings, finn_token)

            # Add Sentiment
            #df_analyzed['ticker'] = df_analyzed['ticker'].apply(lambda x: )

            self.df_analyzed = self.analyze_position(self.df_analyzed, self.capital, self.risk)

        self.df_analyzed = self.df_analyzed[self.df_analyzed['Above200'] == True]
        self.df_analyzed = self.df_analyzed[self.df_analyzed['RSI'] != None]
        self.df_analyzed = self.df_analyzed[self.df_analyzed['Trend'] != None]
        self.df_analyzed = self.df_analyzed[self.df_analyzed['Earnings'] != None]
        self.df_analyzed = self.df_analyzed[self.df_analyzed['Supp/Res'] != None]
        self.df_analyzed = self.df_analyzed[self.df_analyzed['Pullback'] != None]

        return self.rowcount, self.df_analyzed
    
    def update_data(self, ticker, conn, cur, finn_token):
        self.df_insert = pd.DataFrame()
        self.update_tickers = f"""SELECT 
                                    t.ticker, 
                                    COALESCE(EXTRACT(epoch FROM MAX(s.quotedate) AT TIME ZONE 'UTC'), 
                                            EXTRACT('epoch' FROM (CURRENT_DATE - INTERVAL '2 year')::TIMESTAMP AT TIME ZONE 'UTC' ))::bigint::int as from, 
                                    EXTRACT('epoch' FROM CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::bigint::int as to,
                                    MAX(s.quotedate) as maxdate
                                    FROM tickers t
                                    LEFT JOIN stockdata	s	
                                        ON t.ticker = s.ticker
                                    WHERE t.ticker = '{ticker[0]}'
                                    GROUP BY t.ticker
                                """
        self.cur.execute(self.update_tickers, self.conn)
        self.update_tuple = self.cur.fetchall()
        self.date_catch = np.where((datetime.today() - pd.tseries.offsets.BDay(0)) > datetime.today(),
                                   (datetime.today() - pd.tseries.offsets.BDay(1)),
                                   (datetime.today() - pd.tseries.offsets.BDay(0)))
        """It would be intelligent to add a catch for holidays as well."""
        if self.update_tuple[0][3] > self.date_catch:
            print(f"{self.ticker[0]} is up-to-date.")
            
            return None
        
        else:
            self.r = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={self.update_tuple[0][0]}&resolution=D&from={self.update_tuple[0][1]}&to={self.update_tuple[0][2]}&token={self.finn_token}')
            #print(f'https://finnhub.io/api/v1/stock/candle?symbol={self.update_tuple[0][0]}&resolution=D&from={self.update_tuple[0][1]}&to={self.update_tuple[0][2]}&token={self.finn_token}')
            print(self.r.status_code)

            if self.r.status_code == 200:
                self.data = pd.DataFrame(self.r.json())

                del self.data['s']
                self.data.columns = ['close', 'high', 'low', 'open', 'quotedate', 'volume']
                self.data['quotedate'] = self.data['quotedate'].apply(lambda x: datetime.utcfromtimestamp(x).date())
                self.data['ticker'] = self.ticker[0]
                
                self.df_insert = self.df_insert.append(self.data)
            
            elif self.r.status_code == 429:
                time.sleep(30)
                self.r = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={self.ticker[0]}&resolution=D&from={self.ticker[1]}&to={self.ticker[2]}&token={self.finn_token}')
                print(f'https://finnhub.io/api/v1/stock/candle?symbol={self.ticker[0]}&resolution=D&from={self.ticker[1]}&to={self.ticker[2]}&token={self.finn_token}')
                print(self.r.status_code)

                self.data = pd.DataFrame(self.r.json())

                del self.data['s']
                self.data.columns = ['close', 'high', 'low', 'open', 'quotedate', 'volume']
                self.data['quotedate'] = self.data['quotedate'].apply(lambda x: datetime.utcfromtimestamp(x).date())
                self.data['ticker'] = self.ticker[0]
                
                self.df_insert = self.df_insert.append(self.data)

            self.insert = [list(row) for row in self.df_insert.itertuples(index=False)]
            self.sql_insert = """INSERT INTO public.stockdata(close, high, low, open, quotedate, volume, ticker)
                            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""
            
            self.cur.executemany(self.sql_insert, self.insert)
            self.conn.commit()
            self.rowcount = cur.rowcount            
            print(f"Updated {self.rowcount} rows of {self.ticker[0]}.")
            
            return None
        
        

    def get_earnings(self, finn_token):

        self.r = requests.get(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={self.finn_token}')
        print(self.r.status_code)
        
        if self.r.status_code == 200:
            self.df_earnings = pd.DataFrame(self.r.json()['earningsCalendar'])
            self.df_earnings = self.df_earnings[['date','symbol']]

        elif self.r.status_code == 429:
            time.sleep(30)
            self.r = requests.get(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={self.finn_token}')
            print(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={self.finn_token}')
            print(self.r.status_code)
        
            self.df_earnings = pd.DataFrame(self.r.json()['earningsCalendar'])
            self.df_earnings = self.df_earnings[['date','symbol']]
        
        return self.df_earnings

    def get_hist(self, ticker, conn):
        self.get_data = f"""SELECT 
                        ticker
                        ,quotedate as "Date"
                        ,open as "Open"
                        ,high as "High"
                        ,low as "Low"
                        ,close as "Close"
                        ,volume as "Volume"
                        FROM stockdata
                        WHERE ticker = '{self.ticker[0]}'
                        ORDER BY quotedate ASC"""
        #print(self.get_data)
        self.data = pd.read_sql(self.get_data, self.conn)
        
        return self.data    

    def get_indicators(self, data):
        # Get MACD
        self.data["macd"], self.data["macd_signal"], self.data["macd_hist"] = self.talib.MACD(data['Close'])
        
        # Get SMA10 and SMA30
        self.data["sma10"] = self.talib.SMA(self.data["Close"], timeperiod=10)
        self.data["sma30"] = self.talib.SMA(self.data["Close"], timeperiod=30)
        
        # Get MA200
        self.data["sma200"] = self.talib.SMA(self.data["Close"], timeperiod=200)
        
        # Get RSI
        self.data["rsi"] = self.talib.RSI(self.data["Close"])
        
        return self.data

    def analyze_chart(self, ticker, indicated_data, df_analyzed, df_earnings, finn_token):
        
        # Check RSI
        if self.indicated_data.loc[:,'rsi'].iloc[-1] < 35: 
            self.rsi = "Oversold"
        elif indicated_data.loc[:,'rsi'].iloc[-1] > 65: 
            self.rsi = "Overbought"
        else: 
            self.rsi = None

        # Check SMA Trend
        if self.indicated_data.loc[:,'sma30'].iloc[-1]<self.indicated_data.loc[:,'sma10'].iloc[-1]:
            self.trend = "Uptrend"
        elif self.indicated_data.loc[:,'sma30'].iloc[-1]>self.indicated_data.loc[:,'sma10'].iloc[-1]:
            self.trend = "Downtrend"
        else:
            self.trend = None
        
        # Check 200SMA
        if self.indicated_data.loc[:,'Open'].iloc[-1]>self.indicated_data.loc[:,'sma200'].iloc[-1]: 
            self.above200 = True
        else:
            self.above200 = None
        
        # Check for Earnings
        if self.df_earnings['symbol'].str.contains(self.ticker[0]).any():
            self.earnings = pd.to_datetime(self.df_earnings.loc[self.df_earnings['symbol']==self.ticker[0],'date'])
        else:
            self.earnings = 'N/A'
    
        # Check for support or resistance
        self.req = requests.get(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={self.ticker[0]}&resolution=D&token={self.finn_token}')
        # print(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={self.ticker[0]}&resolution=D&token={self.finn_token}')
        print(self.r.status_code)

        if self.r.status_code == 200:
            self.supp_res = None
            self.supp_res_price = float()
            for self.level in self.req.json()['levels']:
                if float(self.level)*0.90 < self.indicated_data.loc[:,'Open'].iloc[-1] < float(self.level)*1.10:
                    if self.indicated_data.loc[:,'Open'].iloc[-1] >= float(self.level):
                        self.supp_res = "support"
                        self.supp_res_price = round(self.level, 2)
                    elif self.indicated_data.loc[:,'Open'].iloc[-1] <= float(self.level):
                        self.supp_res = "resistance"
                        self.supp_res_price = round(self.level, 2)
                    else:
                        self.supp_res = "Indeterminant"
                        self.supp_res_price = None
                else:
                    pass

        elif self.r.status_code == 429:
            time.sleep(30)
            self.req = requests.get(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={self.ticker[0]}&resolution=D&token={self.finn_token}')
            print(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={self.ticker[0]}&resolution=D&token={self.finn_token}')
            print(self.r.status_code)
        
            self.supp_res = None
            self.supp_res_price = float()
            for self.level in self.req.json()['levels']:
                if float(self.level)*0.90 < self.indicated_data.loc[:,'Open'].iloc[-1] < float(self.level)*1.10:
                    if self.indicated_data.loc[:,'Open'].iloc[-1] >= float(self.level):
                        self.supp_res = "support"
                        self.supp_res_price = round(self.level, 2)
                    elif self.indicated_data.loc[:,'Open'].iloc[-1] <= float(self.level):
                        self.supp_res = "resistance"
                        self.supp_res_price = round(self.level, 2)
                    else:
                        self.supp_res = "Indeterminant"
                        self.supp_res_price = None
                else:
                    pass
        # Check TAZ
        # Check for Pullback
        if self.indicated_data.loc[:,'Close'].iloc[-1]<= self.indicated_data.loc[:,'Close'].iloc[-2]<= self.indicated_data.loc[:,'Close'].iloc[-3]:
            self.pullback = True
        else: 
            self.pullback = None

        self.df_analyzed = self.df_analyzed.append({'Ticker' : self.ticker[0], 
                            'Open' : round(self.indicated_data.loc[:,'Open'].iloc[-1]),
                            'Quote' : round(self.indicated_data.loc[:,'Close'].iloc[-1]),
                            'RSI' : self.rsi,
                            'Trend' : self.trend,
                            'Above200' : self.above200,
                            'Earnings' : self.earnings, 
                            'Supp/Res' : self.supp_res,
                            'S/R Price' : self.supp_res_price,
                            'Pullback' : self.pullback#,
                            #'Sentiment' : self.sentiment
                            }, ignore_index=True)
        
        return self.df_analyzed

    def analyze_position(self, df_analyzed, capital, risk):
        self.position_risk = self.capital*self.risk
        
        self.df_analyzed['Entry'] = self.df_analyzed['S/R Price']
        self.df_analyzed['Stoploss'] = self.df_analyzed['S/R Price'].astype(float).apply(lambda x: x * float(0.95))  
        self.df_analyzed['risk_per_share'] = self.df_analyzed['Entry'] - self.df_analyzed['Stoploss']
        self.df_analyzed['position_size'] = round(self.position_risk/self.df_analyzed['risk_per_share'])

        return self.df_analyzed

    def get_sentiment_score(tickers, conn, cur):

        sentiment = Sentiment_FinViz(tickers, conn, cur)
        news_tables = sentiment.Get_News(tickers)
        parsed_news = sentiment.Parse_News(news_tables)
        parsed_and_scored_news = sentiment.Get_Sentiment(parsed_news)
        parsed_and_scored_news = parsed_and_scored_news.loc[parsed_and_scored_news['timestamp']>(datetime.today() - timedelta(7)),:]
        sentiment_score = parsed_and_scored_news.groupby(['ticker']).mean()
