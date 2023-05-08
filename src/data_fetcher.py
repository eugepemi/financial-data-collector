import requests
import yaml

class TestAPI:
    def __init__(self):
        with open('cfg/config.yml') as config_file:
            config_data = yaml.safe_load(config_file)
            self.rapidapi_key = config_data['rapidapi_key']
            self.rapidapi_host = "realstonks.p.rapidapi.com"

    def fetch_data(self, ticker):
        url = f"https://{self.rapidapi_host}/{ticker}"
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": self.rapidapi_host
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code}: Failed to fetch data for {ticker}")
            return None

    def print_data(self, ticker):
        data = self.fetch_data(ticker)
        if data is not None:
            print(data)

if __name__ == "__main__":
    test_api = TestAPI()
    tickers = ["AAPL", "GOOGL", "AMZN", "MSFT"]
    for ticker in tickers:
        test_api.print_data(ticker)
