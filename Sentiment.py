
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import os
import pandas as pd
import nltk 
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import psycopg2 as pg


def __init__(tickers, conn, cur):
    tickers = tickers
    conn = conn
    cur = cur

def Get_News(tickers):
    news_tables = {}

    for ticker in tickers:
        finviz_url = f"https://finviz.com/quote.ashx?t={ticker.replace('.', '-')}"
        
        print(finviz_url)
        req = Request(url=finviz_url,headers={'user-agent': 'my-app/0.0.1'}) 
        response = urlopen(req)    

        # Read the contents of the file into 'html'
        html = BeautifulSoup(response, "html.parser")

        # Find 'news-table' in the Soup and load it into 'news_table'
        news_table = html.find(id='news-table')

        # Add the table to our dictionary
        news_tables[ticker] = news_table
    return news_tables
    
def Parse_News(news_tables):
    parsed_news = []

    # Iterate through the news
    for file_name, news_table in news_tables.items():
        # Iterate through all tr tags in 'news_table'
        for x in news_table.findAll('tr'):
            # read the text from each tr tag into text
            # get text from a only
            text = x.a.get_text() 
            # splite text in the td tag into a list 
            date_scrape = x.td.text.split()
            # if the length of 'date_scrape' is 1, load 'time' as the only element

            if len(date_scrape) == 1:
                time = date_scrape[0]

            # else load 'date' as the 1st element and 'time' as the second    
            else:
                date = date_scrape[0]
                time = date_scrape[1]
            # Extract the ticker from the file name, get the string up to the 1st '_'  
            ticker = file_name.split('_')[0]

            # Append ticker, date, time and headline as a list to the 'parsed_news' list
            parsed_news.append([ticker, date, time, text])
    return parsed_news


def Get_Sentiment(parsed_news):
    vader = SentimentIntensityAnalyzer()

    parsed_and_scored_news = pd.DataFrame(parsed_news, columns = ['ticker', 'date', 'time', 'headline'])

    scores = parsed_and_scored_news['headline'].apply(vader.polarity_scores).tolist()
    df_scores = pd.DataFrame(scores)

    parsed_and_scored_news = parsed_and_scored_news.join(df_scores, rsuffix='_right')
    #parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date
    parsed_and_scored_news['timestamp'] = pd.to_datetime(parsed_and_scored_news['date'] + ' ' + parsed_and_scored_news['time'])
    del parsed_and_scored_news['date'], parsed_and_scored_news['time'], parsed_and_scored_news['neg'], parsed_and_scored_news['neu'],  parsed_and_scored_news['pos'] 

    return parsed_and_scored_news
    
def Load_Sentiment(parsed_and_scored_news, conn, cur):
    # Combine date and time columns and prep for db
    parsed_and_scored_news['timestamp'] = pd.to_datetime(parsed_and_scored_news['date'] + ' ' + parsed_and_scored_news['time'])
    del parsed_and_scored_news['date'], parsed_and_scored_news['time'], parsed_and_scored_news['neg'],  parsed_and_scored_news['neu'],  parsed_and_scored_news['pos'] 

    # Add qualitative scale to scores?
    score_name = {'very positive', 'positive', 'neutral', 'negative', 'very negative'}

    insert = [list(row) for row in parsed_and_scored_news.itertuples(index=False)]

    SQL_sentiment_insert= """ INSERT INTO public.sentiment(ticker, headline, score, timestamp) 
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"""
    cur.executemany(SQL_sentiment_insert, insert)
    conn.commit()

    return None
