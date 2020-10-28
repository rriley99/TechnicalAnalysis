import psycopg2 as pg
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from DataLoading import DataLoading
import TechnicalAnalysis

capital = 100000
risk = 0.05
conn = pg.connect("dbname=StonksGoUp user=postgres host=localhost password=admin")
cur = conn.cursor()

with open('TechnicallySpeaking/local_settings.txt') as f:
    json_local = json.load(f)

finn_token = json_local["finn_token"]

sql_tickers = """SELECT 
                ticker
                FROM tickers
                WHERE isdow='true'
                GROUP BY ticker
                """
cur.execute(sql_tickers,conn)
tickers = cur.fetchall()
print(tickers)

rowcount, df_analyzed = TechnicalAnalysis.do_analysis(conn, cur, finn_token, tickers, capital, risk)
print(df_analyzed)

# sentiment = Sentiment_FinViz(tickers, conn, cur)
# news_tables = sentiment.Get_News(tickers)
# parsed_news = sentiment.Parse_News(news_tables)
# parsed_and_scored_news = sentiment.Get_Sentiment(parsed_news)
# parsed_and_scored_news = parsed_and_scored_news.loc[parsed_and_scored_news['timestamp']>(datetime.today() - timedelta(7)),:]
#print(parsed_and_scored_news.groupby(['ticker']).mean())