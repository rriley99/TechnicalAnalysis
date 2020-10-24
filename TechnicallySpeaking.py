import psycopg2 as pg
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
#from Sentiment import Sentiment_FinViz
from TechnicalAnalysis import TechnicalAnalysis

conn = pg.connect("dbname=StonksGoUp user=postgres host=localhost password=admin")
cur = conn.cursor()

with open('TechnicallySpeaking/local_settings.txt') as f:
    json_local = json.load(f)

finn_token = json_local["finn_token"]
tickers = ['SQ','TSLA']

analysis = TechnicalAnalysis(conn, cur)
update_tickers = analysis.update_data(conn, cur)
#print(update_tickers[:10])







# sentiment = Sentiment_FinViz(tickers, conn, cur)
# news_tables = sentiment.Get_News(tickers)
# parsed_news = sentiment.Parse_News(news_tables)
# parsed_and_scored_news = sentiment.Get_Sentiment(parsed_news)
# parsed_and_scored_news = parsed_and_scored_news.loc[parsed_and_scored_news['timestamp']>(datetime.today() - timedelta(7)),:]
#print(parsed_and_scored_news.groupby(['ticker']).mean())