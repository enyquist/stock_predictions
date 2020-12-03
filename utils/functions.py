from datetime import datetime
import json
import os
import pandas as pd
import requests
import time

RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
RAPIDAPI_HOST = os.environ.get('RAPIDAPI_HOST')


class Stock:
    """
    Stock is designed to collect relevant data for analysis on a given stock, parsing data from the RapidAPI for
    Yahoo Finance.

    Use Guidelines:

    my_stock = Stock('TICKER_SYMBOL')

    my_stock_historical_data = my_stock.df_historical_values - returns a DataFrame with Date, Open, Close, and Dividend
    payouts since first trade date of the stock

    my_stock_statistics = my_stock.dict_statistics - Returns a dictionary with relevant statistics from the
    current quarter

    To Do:
    - Check for null values in API return
    - Check for invalid ticker symbol
    - Reduce number of API calls, currently 3 per instance
    """

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self._jsonHistData = self.fetch_stock_histories()
        self._jsonStatData = self.fetch_stock_statistics()
        self.df_historical_values = self.create_hist_df()
        self.dict_statistics = self.parse_statistics()

    def __repr__(self):
        return f"Stock('{self.symbol}')"

    def __str__(self):
        return f'{self.symbol} stock performance data'

    def fetch_stock_histories(self):
        """
        Function to fetch all historical stock data from Yahoo Finance via Rapid API
        :return: Dictionary of Dictionaries containing all results from history page
        """
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/get-histories"

        current_time = round(time.time())

        first_trade_date = self.fetch_stock_first_trade_date()

        querystring = {"symbol": self.symbol,
                       "from": first_trade_date,
                       "to": current_time,
                       "events": "div",
                       "interval": "1d",
                       "region": "US"}

        headers = {
            'x-rapidapi-key': RAPIDAPI_KEY,
            'x-rapidapi-host': RAPIDAPI_HOST
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        if response.status_code == 200:
            json_content = json.loads(response.content)
            return json_content
        else:
            return None

    def fetch_stock_statistics(self):
        """
        Function to fetch stock statistics from Yahoo Finance via Rapid API
        :return: Dictionary of Dictionaries containing all results from statistics page
        """
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-statistics"

        querystring = {"symbol": self.symbol,
                       "region": "US"}

        headers = {
            'x-rapidapi-key': RAPIDAPI_KEY,
            'x-rapidapi-host': RAPIDAPI_HOST
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        if response.status_code == 200:
            json_content = json.loads(response.content)
            return json_content
        else:
            return None

    def fetch_stock_first_trade_date(self):
        """
        Function to fetch a stock's first trade date from the meta data
        :return: Return epoch timestamp of the first recorded trade
        """
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-chart"

        querystring = {"interval": "5m",
                       "symbol": self.symbol,
                       "range": "1d",
                       "region": "US"}

        headers = {
            'x-rapidapi-key': RAPIDAPI_KEY,
            'x-rapidapi-host': RAPIDAPI_HOST
        }

        response = requests.request("GET", url, headers=headers, params=querystring)

        if response.status_code == 200:
            json_content = json.loads(response.content)
            first_trade_date = json_content['chart']['result'][0]['meta']['firstTradeDate']
            return first_trade_date
        else:
            return None

    def parse_timestamp(self):
        """
        Parse the timestamps from the Rapid API call into calendar time
        :return: List of datetime objects correlating to each day of trading
        """

        list_timestamps = []

        list_timestamps.extend(self._jsonHistData['chart']['result'][0]['timestamp'])

        list_calendar_time = []

        for ts in list_timestamps:
            dt = datetime.fromtimestamp(ts)
            list_calendar_time.append(dt.strftime('%m/%d/%Y'))

        return list_calendar_time

    def parse_statistics(self):
        """
        Parse the statistics from the Rapid API call
        :return: Dictionary containing key/value pairs with statistics from the current quarter
        """

        dict_stats = {
            'payout_ratio': self._jsonStatData['summaryDetail']['payoutRatio']['raw'],
            'dividend_rate': self._jsonStatData['summaryDetail']['dividendRate']['raw'],
            'beta': self._jsonStatData['summaryDetail']['beta']['raw'],
            'trailing_pe': self._jsonStatData['summaryDetail']['trailingPE']['raw'],
            'forward_pe': self._jsonStatData['summaryDetail']['forwardPE']['raw'],
            'five_year_avg_div_yield': self._jsonStatData['summaryDetail']['fiveYearAvgDividendYield']['raw'],
            'div_yield': self._jsonStatData['summaryDetail']['dividendYield']['raw'],
            'forward_eps': self._jsonStatData['defaultKeyStatistics']['forwardEps']['raw'],
            'trailing_eps': self._jsonStatData['defaultKeyStatistics']['trailingEps']['raw'],
            'ebitda_margins': self._jsonStatData['financialData']['ebitdaMargins']['raw'],
            'profit_margins': self._jsonStatData['financialData']['profitMargins']['raw'],
            'gross_margins': self._jsonStatData['financialData']['grossMargins']['raw'],
            'op_cashflow': self._jsonStatData['financialData']['operatingCashflow']['raw'],
            'revenue_growth': self._jsonStatData['financialData']['revenueGrowth']['raw'],
            'operating_margins': self._jsonStatData['financialData']['operatingMargins']['raw'],
            'gross_profits': self._jsonStatData['financialData']['grossProfits']['raw'],
            'free_cashflow': self._jsonStatData['financialData']['freeCashflow']['raw'],
            'earnings_growth': self._jsonStatData['financialData']['earningsGrowth']['raw'],
            'page_short_term_trend': self._jsonStatData['pageViews']['shortTermTrend'],
            'page_mid_term_trend': self._jsonStatData['pageViews']['midTermTrend'],
            'page_long_term_trend': self._jsonStatData['pageViews']['longTermTrend']
        }

        return dict_stats

    def parse_values(self):
        """
        Parse the values from the Rapid API call
        :return: Return two lists, list_open with values at open and list_close with values at close as floats
        """
        list_open = []
        list_close = []
        list_open.extend(self._jsonHistData['chart']['result'][0]['indicators']['quote'][0]['open'])
        list_close.extend(self._jsonHistData['chart']['result'][0]['indicators']['quote'][0]['close'])

        return list_open, list_close

    def create_hist_df(self):
        """
        Create DataFrame of historical values from self._jsonHistData
        :return: DataFrame with four columns, 'Date' as datetime and 'Open' / 'Close' / 'Dividend Amount' as float
        """

        data = {'Date': self.parse_timestamp(), 'Open': self.parse_values()[0], 'Close': self.parse_values()[1]}

        df_hist = pd.DataFrame(data)

        df_div = self.parse_dividends()

        df_master = df_hist.merge(df_div, how='outer', on='Date')

        df_master['Dividend Amount'] = df_master['Dividend Amount'].fillna(0)

        return df_master

    def parse_dividends(self):
        """
        Parse dividends from Rapid API call
        :return: DataFrame containing two columns, 'Date' as datetime and 'Dividend Amount' as float
        """

        dict_div = self._jsonHistData['chart']['result'][0]['events']['dividends']

        list_div_dates = []
        list_div_amounts = []

        for key in dict_div.keys():
            list_div_dates.append(dict_div[key]['date'])
            list_div_amounts.append(dict_div[key]['amount'])

        list_calendar_time = []

        for ts in list_div_dates:
            dt = datetime.fromtimestamp(ts)
            list_calendar_time.append(dt.strftime('%m/%d/%Y'))

        data = {'Date': list_calendar_time, 'Dividend Amount': list_div_amounts}

        df_div = pd.DataFrame(data)

        return df_div
