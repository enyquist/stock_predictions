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

    """

    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self._firstTradeDate = self.fetch_stock_first_trade_date()
        self._jsonData = self.fetch_stock_histories()
        self._data = {}
        self.df_historical_values = self.create_hist_df()
        # self.df_historical_dividends = self.create_div_df()

    def __repr__(self):
        return f"Stock('{self.symbol}')"

    def __str__(self):
        return f'{self.symbol} historical data'

    def fetch_stock_histories(self):
        """
        Function to fetch all historical stock data from Yahoo Finance via Rapid API
        :return:
        """
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/get-histories"

        current_time = round(time.time())

        querystring = {"symbol": self.symbol,
                       "from": self._firstTradeDate,
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

    def fetch_stock_first_trade_date(self):
        """
        Function to fetch a stock's first trade date from the meta data
        :return:
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
        :return:
        """

        timestamp_list = []

        timestamp_list.extend(self._jsonData['chart']['result'][0]['timestamp'])

        calendar_time = []

        for ts in timestamp_list:
            dt = datetime.fromtimestamp(ts)
            calendar_time.append(dt.strftime('%m/%d/%Y'))

        return calendar_time

    def parse_values(self):
        """
        Parse the values from the Rapid API call
        :return:
        """
        open_list = []
        close_list = []
        open_list.extend(self._jsonData['chart']['result'][0]['indicators']['quote'][0]['open'])
        close_list.extend(self._jsonData['chart']['result'][0]['indicators']['quote'][0]['close'])

        return open_list, close_list

    def create_hist_df(self):
        """
        Create dataframe of historical values from json data
        :return:
        """

        self._data['Date'] = self.parse_timestamp()
        self._data['Open'], self._data['Close'] = self.parse_values()

        df = pd.DataFrame(self._data)

        return df

    def parse_dividends(self):
        """
        Parse dividends from Rapid API call
        :return:
        """

        dividend_dict = self._jsonData['chart']['result'][0]['events']['dividends']

        dividend_list = [value for value in dividend_dict.values()]

        dividend_dates = []
        dividend_values = []

        for dict_ in dividend_list:
            for key, value in dict_.items():
                dividend_values.append(key)
                dividend_dates.append(value)

    def create_div_df(self):
        """
        Create dataframe of historical divdends from json data
        :return:
        """