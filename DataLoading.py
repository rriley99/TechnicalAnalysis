import psycopg2
import pandas as pd
import yahoo_fin.stock_info as si
import time

class DataLoading:
    def __init__(self, cur, conn):
        self.cur = cur
        self.conn = conn
        self.SQL_check = check_tables(cur, conn)

        """Work on logic for creating table if it doesn't exist"""
        #self.tables_returned = check_tables(cur, conn)
        #create_tables(self, cur, conn)
        #load_tickers(self, cur, conn)
        #load_hist(self, cur, conn)

        return print(self.SQL_check)

    def check_tables(self, cur, conn):
        self.tables = ('tickers', 'stockdata')
        self.SQL_check = f"""
                            SELECT tablename
                            FROM pg_catalog.pg_tables
                            WHERE schemaname != 'pg_catalog' 
                            AND schemaname != 'information_schema'
                            AND tablename IN ({tables});
                            """
        
        self.cur.execute(self.SQL_check, self.conn)
        self.tables_returned = [x[0] for x in self.cur.fetchall()]
        
        return self.tables_returned
        
    def create_tables(self, cur, conn):
        self.SQL_stockdata = """ 
            CREATE TABLE stockdata (
                ticker varchar(5) NOT NULL,
                quotedate date NOT NULL,
                open numeric NOT NULL,
                high numeric NOT NULL,
                low numeric NOT NULL,
                close numeric NOT NULL,
                adjclose numeric NOT NULL,
                volume bigint,
                CONSTRAINT pk_stockdata PRIMARY KEY (ticker, quotedate)
            );
            """
        self.SQL_tickers = """
            CREATE TABLE tickers (
            ticker varchar(10) NOT NULL,
            isdow boolean NOT NULL DEFAULT false,
            isnasdaq boolean NOT NULL DEFAULT false,
            issp500 boolean NOT NULL DEFAULT false,
            createddate timestamp with time zone NOT NULL DEFAULT now(),
            CONSTRAINT pk_tickers PRIMARY KEY (ticker)
                );
            """
        self.cur.execute(self.SQL_stockdata, self.conn)
        self.cur.execute(self.SQL_tickers, self.conn)
        self.conn.commit()

        return None

    def load_tickers(self, cur, conn):

        self.df_ticker = pd.DataFrame()
        self.nasdaq = si.tickers_nasdaq()
        self.sp500 = si.tickers_sp500()
        self.dow = si.tickers_dow()
        self.other = si.tickers_other()

        self.tickers = list(set([*nasdaq, *sp500, *dow, *other]))

        self.df_ticker['ticker'] = self.tickers
        self.df_ticker.replace("", float("NaN"), inplace=True)
        self.df_ticker.dropna(subset = ["ticker"], inplace=True)

        self.df_ticker['isdow'] = self.df_ticker['ticker'].isin(self.dow)
        self.df_ticker['issp500'] = self.df_ticker['ticker'].isin(self.sp500)
        self.df_ticker['isnasdaq'] = self.df_ticker['ticker'].isin(self.nasdaq)

        self.insert = [list(row) for row in self.df_ticker.itertuples(index=False)]

        self.SQL_Ticker_insert= """ INSERT INTO public.tickers(ticker,isdow, isnasdaq, issp500) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
        self.cur.executemany(self.SQL_Ticker_insert, self.insert)
        self.conn.commit()
        self.rowcount = self.cur.rowcount
        
        return None

    def load_hist(self, cur, conn):
        
        self.SQL_stockdata_insert = """INSERT INTO public.stockdata (quotedate, open, high, low, close, adjclose, volume, ticker)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
        
        for self.ticker in self.df_ticker['ticker']:
            time.sleep(3)
            
            try: 
                self.data = si.get_data(self.ticker)
                self.insert_data = [list(row) for row in self.data.itertuples()]
                self.cur.executemany(self.SQL_stockdata_insert, self.insert_data)
            
            except: pass
            
            conn.commit()  
        
        return None

DataLoading