import yahoo_fin.stock_info as si
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
import talib
import requests
import json
import openpyxl
import psycopg2 as pg
from Sentiment import *
### Work to add ###
# (1) Consider adding holidays to date_catch functinality
# (2) Get count of requested stocks that are in need of an update, instead of print "x is up to date"
# (3) Add Sentiment
# (4) Add ML resistance
# (5) Add ML trend

def __init__(conn, cur, finn_token, tickers, capital, risk):
    talib = talib
    conn = conn
    cur = cur
    finn_token = finn_token
    capital = capital
    risk = risk
    tickers = tickers
      
def do_analysis(conn, cur, finn_token, tickers, capital, risk):    
    df_analyzed = pd.DataFrame(columns=['Ticker', 'Open', 'Quote', 'RSI', 'Trend', 'Above200', 'Earnings', 'Supp/Res', 'S/R Price', 'Pullback']) 
    # get earnings to avoid over clocking the API
    df_earnings = get_earnings(finn_token)
    
    for ticker in tickers:
        update_data(ticker, conn, cur, finn_token)
        
        print(f"Analyzing {ticker[0]}")
        # Get historical data
        data = get_hist(ticker, conn)

        # Add indicator data
        indicated_data = get_indicators(data)

        # Analyze stonks:
        
        df_analyzed = analyze_chart(ticker, indicated_data, df_analyzed, df_earnings, finn_token)

        # Add Sentiment
        #df_analyzed['ticker'] = df_analyzed['ticker'].apply(lambda x: )

        df_analyzed = analyze_position(df_analyzed, capital, risk)

    df_analyzed = df_analyzed[df_analyzed['Above200'] == True]
    df_analyzed = df_analyzed[df_analyzed['RSI'] != None]
    df_analyzed = df_analyzed[df_analyzed['Trend'] != None]
    #df_analyzed = df_analyzed[df_analyzed['Earnings'] != None]
    df_analyzed = df_analyzed[df_analyzed['Supp/Res'] != None]
    df_analyzed = df_analyzed[df_analyzed['Pullback'] != None]

    # Add Sentiment
    sentiment_scores = get_sentiment_score(df_analyzed['Ticker'], conn, cur)
    sentiment_scores.rename(columns={"ticker": "Ticker", "compound": "FinViz Sentiment"}, inplace=True)
    df_analyzed = df_analyzed.join(sentiment_scores, on = ['Ticker'])

    return df_analyzed

def update_data(ticker, conn, cur, finn_token):
    df_insert = pd.DataFrame()
    update_tickers = f"""SELECT 
                                t.ticker, 
                                COALESCE(EXTRACT(epoch FROM MAX(s.quotedate) AT TIME ZONE 'UTC'), 
                                        EXTRACT('epoch' FROM (CURRENT_DATE - INTERVAL '2 year')::TIMESTAMP AT TIME ZONE 'UTC' ))::bigint::int as from, 
                                EXTRACT('epoch' FROM CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::bigint::int as to,
                                COALESCE(MAX(s.quotedate)+interval '1 day', CURRENT_DATE - INTERVAL '2 year') as maxdate
                                FROM tickers t
                                LEFT JOIN stockdata	s	
                                    ON t.ticker = s.ticker
                                WHERE t.ticker = '{ticker[0]}'
                                GROUP BY t.ticker
                            """
    cur.execute(update_tickers, conn)
    update_tuple = cur.fetchall()
    catch = np.where((datetime.today() - pd.tseries.offsets.BDay(0)) > datetime.today(),
                                (datetime.today() - pd.tseries.offsets.BDay(1)),
                                (datetime.today() - pd.tseries.offsets.BDay(0)))
    date_catch = datetime.strptime(str(catch), '%Y-%m-%d %H:%M:%S.%f').date()
    """It would be intelligent to add a catch for holidays as well."""
    # print(update_tuple[0][3])
    # print(date_catch)
    if update_tuple[0][3].date() >= date_catch:
        #print(f"{ticker[0]} is up-to-date.")
        return None
    
    else:
        req = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={update_tuple[0][0]}&resolution=D&from={update_tuple[0][1]}&to={update_tuple[0][2]}&token={finn_token}')
        # print(f'https://finnhub.io/api/v1/stock/candle?symbol={update_tuple[0][0]}&resolution=D&from={update_tuple[0][1]}&to={update_tuple[0][2]}&token={finn_token}')
        # print(req.status_code)

        if req.status_code == 200:
            data = pd.DataFrame(req.json())

            del data['s']
            data.columns = ['close', 'high', 'low', 'open', 'quotedate', 'volume']
            data['quotedate'] = data['quotedate'].apply(lambda x: datetime.utcfromtimestamp(x).date())
            data['ticker'] = ticker[0]
            
            df_insert = df_insert.append(data)
        
        elif req.status_code == 429:
            time.sleep(30)
            req = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={update_tuple[0][0]}&resolution=D&from={update_tuple[0][1]}&to={update_tuple[0][2]}&token={finn_token}')
            print(f'https://finnhub.io/api/v1/stock/candle?symbol={update_tuple[0][0]}&resolution=D&from={update_tuple[0][1]}&to={update_tuple[0][2]}&token={finn_token}')
            print(req.status_code)

            data = pd.DataFrame(req.json())

            del data['s']
            data.columns = ['close', 'high', 'low', 'open', 'quotedate', 'volume']
            data['quotedate'] = data['quotedate'].apply(lambda x: datetime.utcfromtimestamp(x).date())
            data['ticker'] = ticker[0]
            
            df_insert = df_insert.append(data)

        insert = [list(row) for row in df_insert.itertuples(index=False)]
        sql_insert = """INSERT INTO public.stockdata(close, high, low, open, quotedate, volume, ticker)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""
        
        cur.executemany(sql_insert, insert)
        conn.commit()
        rowcount = cur.rowcount            
        print(f"Updated {rowcount} rows of {ticker[0]}.")
        
        return None

def get_earnings(finn_token):

    req = requests.get(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={finn_token}')
    # print(req.status_code)
    
    if req.status_code == 200:
        df_earnings = pd.DataFrame(req.json()['earningsCalendar'])
        df_earnings = df_earnings[['date','symbol']]

    elif req.status_code == 429:
        time.sleep(30)
        req = requests.get(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={finn_token}')
        print(f'https://finnhub.io/api/v1/calendar/earnings?from={datetime.now()-timedelta(days=7)}&to={datetime.now() + timedelta(days=7)}&token={finn_token}')
        print(req.status_code)
    
        df_earnings = pd.DataFrame(req.json()['earningsCalendar'])
        df_earnings = df_earnings[['date','symbol']]
    
    df_earnings['date'] = df_earnings['date'].apply(lambda x: datetime.strptime(str(x),'%Y-%m-%d'))

    return df_earnings

def get_hist(ticker, conn):
    get_data = f"""SELECT 
                    ticker
                    ,quotedate as "Date"
                    ,open as "Open"
                    ,high as "High"
                    ,low as "Low"
                    ,close as "Close"
                    ,volume as "Volume"
                    FROM stockdata
                    WHERE ticker = '{ticker[0]}'
                    ORDER BY quotedate ASC"""
    #print(get_data)
    data = pd.read_sql(get_data, conn)
    
    return data    

def get_indicators(data):
    # Get MACD
    data["macd"], data["macd_signal"], data["macd_hist"] = talib.MACD(data['Close'])
    
    # Get SMA10 and SMA30
    data["sma10"] = talib.SMA(data["Close"], timeperiod=10)
    data["sma30"] = talib.SMA(data["Close"], timeperiod=30)
    
    # Get MA200
    data["sma200"] = talib.SMA(data["Close"], timeperiod=200)
    
    # Get RSI
    data["rsi"] = talib.RSI(data["Close"])
    
    return data

def analyze_chart(ticker, indicated_data, df_analyzed, df_earnings, finn_token):
    
    # Check RSI
    if indicated_data.loc[:,'rsi'].iloc[-1] < 35: 
        rsi = "Oversold"
    elif indicated_data.loc[:,'rsi'].iloc[-1] > 65: 
        rsi = "Overbought"
    else: 
        rsi = None

    # Check SMA Trend
    if indicated_data.loc[:,'sma30'].iloc[-1]<indicated_data.loc[:,'sma10'].iloc[-1]:
        trend = "Uptrend"
    elif indicated_data.loc[:,'sma30'].iloc[-1]>indicated_data.loc[:,'sma10'].iloc[-1]:
        trend = "Downtrend"
    else:
        trend = None
    
    # Check 200SMA
    if indicated_data.loc[:,'Open'].iloc[-1]>indicated_data.loc[:,'sma200'].iloc[-1]: 
        above200 = True
    else:
        above200 = None
    
    # Check for Earnings
    if df_earnings['symbol'].str.fullmatch(ticker[0]).any():
        
        earnings = pd.Timestamp(df_earnings.where(df_earnings['symbol']==ticker[0].replace('.', '/')).dropna()['date'].values[0]).date()
    else:
        earnings = 'N/A'
    
    # Check for support or resistance
    req = requests.get(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={ticker[0]}&resolution=D&token={finn_token}')
    print(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={ticker[0]}&resolution=D&token={finn_token}')

    supp_res = None
    supp_res_price = float()

    if req.status_code == 429:
        time.sleep(30)
        req = requests.get(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={ticker[0]}&resolution=D&token={finn_token}')
        # print(f'https://finnhub.io/api/v1/scan/support-resistance?symbol={ticker[0]}&resolution=D&token={finn_token}')
        print(req.status_code)
    
        for level in req.json()['levels']:
            if float(level)*0.90 < indicated_data.loc[:,'Open'].iloc[-1] < float(level)*1.10:
                if indicated_data.loc[:,'Open'].iloc[-1] >= float(level):
                    supp_res = "support"
                    supp_res_price = round(level, 2)
                elif indicated_data.loc[:,'Open'].iloc[-1] <= float(level):
                    supp_res = "resistance"
                    supp_res_price = round(level, 2)
                else:
                    supp_res = "Indeterminant"
                    supp_res_price = None
            else:
                pass

    elif len(req.json()) > 0 & req.status_code == 200:
        for level in req.json()['levels']:
            if float(level)*0.90 < indicated_data.loc[:,'Open'].iloc[-1] < float(level)*1.10:
                if indicated_data.loc[:,'Open'].iloc[-1] >= float(level):
                    supp_res = "support"
                    supp_res_price = round(level, 2)
                elif indicated_data.loc[:,'Open'].iloc[-1] <= float(level):
                    supp_res = "resistance"
                    supp_res_price = round(level, 2)
                else:
                    supp_res = "Indeterminant"
                    supp_res_price = None
            else:
                pass
    
    elif len(req.json()) == 0 & req.status_code == 200:
        supp_res = "Indeterminant"
        supp_res_price = None
    
    # Check TAZ
    # Check for Pullback
    if indicated_data.loc[:,'Close'].iloc[-1]<= indicated_data.loc[:,'Close'].iloc[-2]<= indicated_data.loc[:,'Close'].iloc[-3]:
        pullback = True
    else: 
        pullback = None

    df_analyzed = df_analyzed.append({'Ticker' : ticker[0], 
                        'Open' : round(indicated_data.loc[:,'Open'].iloc[-1]),
                        'Quote' : round(indicated_data.loc[:,'Close'].iloc[-1]),
                        'RSI' : rsi,
                        'Trend' : trend,
                        'Above200' : above200,
                        'Earnings' : earnings, 
                        'Supp/Res' : supp_res,
                        'S/R Price' : supp_res_price,
                        'Pullback' : pullback#,
                        #'Sentiment' : sentiment
                        }, ignore_index=True)
    
    return df_analyzed

def analyze_position(df_analyzed, capital, risk):
    position_risk = capital*risk
    
    df_analyzed['Entry'] = df_analyzed['S/R Price']
    df_analyzed['Stoploss'] = df_analyzed['S/R Price'].astype(float).apply(lambda x: x * float(0.95))  
    df_analyzed['risk_per_share'] = df_analyzed['Entry'] - df_analyzed['Stoploss']
    df_analyzed['position_size'] = round(float(position_risk)/df_analyzed['risk_per_share'].astype(float))

    return df_analyzed

def get_sentiment_score(tickers, conn, cur):

    news_tables = Get_News(tickers)
    parsed_news = Parse_News(news_tables)
    parsed_and_scored_news = Get_Sentiment(parsed_news)
    parsed_and_scored_news = parsed_and_scored_news.loc[parsed_and_scored_news['timestamp']>(datetime.today() - timedelta(7)),:]
    sentiment_scores = parsed_and_scored_news.groupby(['ticker']).mean()

    return sentiment_scores

