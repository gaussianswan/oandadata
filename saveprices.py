import tpqoa
import pandas as pd
import datetime

def pull_all_instrument_hist_data(start: str, end: str, price_type: str) -> dict:
    """Pulls all the historical daily data for all instruments in the OANDA platform.

    Args:
        start (str): Starting date in string format. Must be in %m-%d-%y format
        end (str): Ending date in string format. Must be in %m-%d-%y format
        price_type (str): 'B' for bid, 'A' for ask, and 'M' for mid price

    Returns:
        dict: Dictionary where the keys are the instruments and the values are dataframes of the candles
    """
    api = tpqoa.tpqoa('oanda.cfg')
    all_instruments = dict(api.get_instruments())
    data = {}

    for instrument in all_instruments.values():
        df = api.get_history(
            instrument=instrument,
            start=start,
            end=end,
            price=price_type,
            granularity='D'
        )

        data[instrument] = df

    return data

def calculate_spreads_df() -> dict:
    spread_data = {}
    api = tpqoa.tpqoa('oanda.cfg')
    all_instruments = dict(api.get_instruments())

    for instrument in all_instruments.values():

        bid_prices = pd.read_csv("historical_daily_bid_prices/{}_daily_bids.csv".format(instrument), index_col = 0)
        ask_prices = pd.read_csv("historical_daily_ask_prices/{}_daily_asks.csv".format(instrument), index_col = 0)
        spreads = ask_prices[['c']] - bid_prices[['c']]
        spreads.columns = ['spread']
        spread_data[instrument] = spreads

    return spread_data


def mass_save_ohlc_data(data_dict: dict, price_type: str):
    price_type_lowercase = price_type.lower()
    assert price_type_lowercase in ['mid', 'bid', 'ask'], "You can only have price types with name 'bid', 'mid', or 'ask'. You entered: {}".format(price_type)
    for instrument, data in data_dict.items():
        filepath = 'historical_daily_{}_prices/{}_daily_{}s.csv'.format(price_type_lowercase, instrument, price_type_lowercase)
        data.to_csv(filepath)

def mass_save_spread_data(data_dict: dict):

    for instrument, data in data_dict.items():
        filepath = 'historical_daily_spreads/{}_daily_spreads.csv'.format(instrument)
        data.to_csv(filepath)


if __name__ == "__main__":
    start = '2009-01-01'
    end = datetime.date.today().strftime("%Y-%m-%d")
    bid_prices = pull_all_instrument_hist_data(start=start, end=end, price_type='B')
    ask_prices = pull_all_instrument_hist_data(start=start, end=end, price_type='A')
    mid_prices = pull_all_instrument_hist_data(start=start, end=end, price_type='M')


    mass_save_ohlc_data(bid_prices, price_type='bid')
    mass_save_ohlc_data(ask_prices, price_type='ask')
    mass_save_ohlc_data(mid_prices, price_type='mid')

    spreads = calculate_spreads_df()
    mass_save_spread_data(spreads)

    print("All done")

