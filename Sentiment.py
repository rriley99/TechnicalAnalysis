
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import os
import pandas as pd
import datetime
import nltk 
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import psycopg2 as pg

class Sentiment_FinViz:
    
    def __init__(self, tickers, conn, cur):
        self.tickers = tickers
        self.conn = conn
        self.cur = cur
    def Get_News(self, tickers):
        self.news_tables = {}

        for self.ticker in tickers:
            self.finviz_url = f'https://finviz.com/quote.ashx?t={self.ticker}'
            
            print(self.finviz_url)
            self.req = Request(url=self.finviz_url,headers={'user-agent': 'my-app/0.0.1'}) 
            self.response = urlopen(self.req)    

            # Read the contents of the file into 'html'
            self.html = BeautifulSoup(self.response)

            # Find 'news-table' in the Soup and load it into 'news_table'
            self.news_table = self.html.find(id='news-table')

            # Add the table to our dictionary
            self.news_tables[self.ticker] = self.news_table
        return self.news_tables
     
    def Parse_News(self, news_tables):
        self.parsed_news = []

        # Iterate through the news
        for self.file_name, self.news_table in news_tables.items():
            # Iterate through all tr tags in 'news_table'
            for self.x in self.news_table.findAll('tr'):
                # read the text from each tr tag into text
                # get text from a only
                self.text = self.x.a.get_text() 
                # splite text in the td tag into a list 
                self.date_scrape = self.x.td.text.split()
                # if the length of 'date_scrape' is 1, load 'time' as the only element

                if len(self.date_scrape) == 1:
                    self.time = self.date_scrape[0]

                # else load 'date' as the 1st element and 'time' as the second    
                else:
                    self.date = self.date_scrape[0]
                    self.time = self.date_scrape[1]
                # Extract the ticker from the file name, get the string up to the 1st '_'  
                self.ticker = self.file_name.split('_')[0]

                # Append ticker, date, time and headline as a list to the 'parsed_news' list
                self.parsed_news.append([self.ticker, self.date, self.time, self.text])
        return self.parsed_news

    
    def Get_Sentiment(self, parsed_news):
        self.vader = SentimentIntensityAnalyzer()

        self.parsed_and_scored_news = pd.DataFrame(parsed_news, columns = ['ticker', 'date', 'time', 'headline'])

        self.scores = self.parsed_and_scored_news['headline'].apply(self.vader.polarity_scores).tolist()
        self.df_scores = pd.DataFrame(self.scores)

        self.parsed_and_scored_news = self.parsed_and_scored_news.join(self.df_scores, rsuffix='_right')
        #parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date
        self.parsed_and_scored_news['timestamp'] = pd.to_datetime(self.parsed_and_scored_news['date'] + ' ' + self.parsed_and_scored_news['time'])
        del self.parsed_and_scored_news['date'], self.parsed_and_scored_news['time'], self.parsed_and_scored_news['neg'], self.parsed_and_scored_news['neu'],  self.parsed_and_scored_news['pos'] 

        return self.parsed_and_scored_news
        
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
    