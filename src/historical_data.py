# Forked from https://github.com/David-Woroniuk/Historic_Crypto

import requests
import json
import time
from random import randint
import sys
from datetime import datetime, timedelta
from storage_ingestion import AzureBlobStorage


class HistoricalDataRetriever(object):
    """
    This class provides methods for gathering historical price data of a specified
    Cryptocurrency between user specified time periods. The class utilises the CoinBase Pro
    API to extract historical data, providing a performant method of data extraction.

    Args:
        ticker (str): a singular Cryptocurrency ticker. 
        granularity (int): the price data frequency in seconds, one of: 60, 300, 900, 3600, 21600, 86400. 
        start_date (str): a date string in the format YYYY-MM-DD-HH-MM. 
        end_date (str): a date string in the format YYYY-MM-DD-HH-MM, Default=Now. 
    
    Returns:
        data (list): a list of JSON which contains requested cryptocurrency data.
    """
    def __init__(self,
                 ticker,
                 granularity,
                 start_date,
                 end_date=None):

        if not all(isinstance(v, str) for v in [ticker, start_date]):
            raise TypeError("The 'ticker' and 'start_date' arguments must be strings or None types.")
        if not isinstance(end_date, (str, type(None))):
            raise TypeError("The 'end_date' argument must be a string or None type.")
        if isinstance(granularity, int) is False:
            raise TypeError("'granularity' must be an integer object.")
        if granularity not in [60, 300, 900, 3600, 21600, 86400]:
            raise ValueError("'granularity' argument must be one of 60, 300, 900, 3600, 21600, 86400 seconds.")
        if not end_date:
            end_date = datetime.today().strftime("%Y-%m-%d-%H-%M")
        else:
            end_date_datetime = datetime.strptime(end_date, '%Y-%m-%d-%H-%M')
            start_date_datetime = datetime.strptime(start_date, '%Y-%m-%d-%H-%M')
            while start_date_datetime >= end_date_datetime:
                raise ValueError("'end_date' argument cannot occur prior to the start_date argument.")

        self.ticker = ticker
        self.granularity = granularity
        self.start_date = start_date
        self.start_date_string = None
        self.end_date = end_date
        self.end_date_string = None

    def _date_cleaner(self, date_time: datetime):
        """This helper function presents the input as a datetime in the API required format."""
        
        if not isinstance(date_time, (datetime, str)):
            raise TypeError("The 'date_time' argument must be a datetime type.")
        if isinstance(date_time, str):
            output_date = datetime.strptime(date_time, '%Y-%m-%d-%H-%M').isoformat()
        else:
            output_date = date_time.strftime("%Y-%m-%d, %H:%M:%S")
            output_date = output_date[:10] + 'T' + output_date[12:]
        return output_date

    def retrieve_data(self):
        """This function returns the data."""

        self.start_date_string = self._date_cleaner(self.start_date)
        self.end_date_string = self._date_cleaner(self.end_date)
        start = datetime.strptime(self.start_date, "%Y-%m-%d-%H-%M")
        end = datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M")
        request_volume = abs((end - start).total_seconds()) / self.granularity

        data = []
        if request_volume <= 300:
            response = requests.get(
                "https://api.pro.coinbase.com/products/{0}/candles?start={1}&end={2}&granularity={3}".format(
                    self.ticker,
                    self.start_date_string,
                    self.end_date_string,
                    self.granularity))
            if response.status_code not in [200, 201, 202, 203, 204]:
                print("Status Code: {}, malformed request to the CoinBase Pro API.".format(response.status_code))
                sys.exit()
            else:
                raw_data = json.loads(response.text)
                for entry in raw_data:
                    timestamp = datetime.fromtimestamp(entry[0])
                    if start <= timestamp <= end:
                        data.append({"time": timestamp, "low": entry[1], "high": entry[2], "open": entry[3],
                                    "close": entry[4], "volume": entry[5]})
                return data
        else:
            # The API limit:
            max_per_mssg = 300
            for i in range(int(request_volume / max_per_mssg) + 1):
                provisional_start = start + timedelta(0, i * (self.granularity * max_per_mssg))
                provisional_start = self._date_cleaner(provisional_start)
                provisional_end = start + timedelta(0, (i + 1) * (self.granularity * max_per_mssg))
                provisional_end = self._date_cleaner(provisional_end)

                response = requests.get(
                    "https://api.pro.coinbase.com/products/{0}/candles?start={1}&end={2}&granularity={3}".format(
                        self.ticker,
                        provisional_start,
                        provisional_end,
                        self.granularity))

                if response.status_code not in [200, 201, 202, 203, 204]:
                    print("Status Code: {}, malformed request to the CoinBase Pro API.".format(response.status_code))
                    sys.exit()
                else:
                    raw_data = json.loads(response.text)
                    if raw_data:
                        for entry in raw_data:
                            timestamp = datetime.fromtimestamp(entry[0])
                            print(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                            if start <= timestamp <= end:
                                data.append({"time": timestamp.strftime("%Y-%m-%d %H:%M:%S"), "low": entry[1], 
                                             "high": entry[2], "open": entry[3],
                                             "close": entry[4], "volume": entry[5]})
                        time.sleep(randint(0, 2))
                    else:
                        print("""CoinBase Pro API did not have available data for '{}' beginning at {}.  
                        Trying a later date:'{}'""".format(self.ticker, self.start_date, provisional_start))
                        time.sleep(randint(0, 1))
            return data

        
if __name__ == "__main__":
    # Create object of AzureBlobStorage
    ingestor = AzureBlobStorage()

    # Retrieve data from a ticker
    data_retriever = HistoricalDataRetriever('BTC-USD', 300, '2019-05-12-00-00', '2023-05-11-00-00')
    data = data_retriever.retrieve_data()

    # Ingest data into historical container
    ingestor.insert_data(data, 'historical')
