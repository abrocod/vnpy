import pandas as pd
import numpy as np
from datetime import datetime

from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import database_manager


def load_fr(file_path, symbol):
    header_list = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    es_df = pd.read_csv(file_path, 
                        header=None,
                        names = header_list)

    # --- convert string time into timestamp format ----
    # use pandas datetime: output has pandas format
    #   has problem with timezone conversion 
    es_df['DateTime'] =  pd.to_datetime(es_df['DateTime'])

    # use native python datetime lib: should be fine ... debug
    # es_df['DateTime'] = es_df['DateTime'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

    # print out a sample:
    # for ix, row in es_df.iloc[0:30].iterrows():
    #     print(row.DateTime, row.Open)

    exchange = Exchange.CME
    interval = Interval.HOUR
    barlist = []
    for ix, row in es_df.iterrows():
        # print(ix, row)
        bar = BarData(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            datetime=row.DateTime,
            open_price=row.Open,
            high_price=row.High,
            low_price=row.Low,
            close_price=row.Close,
            volume=row.Volume,
            gateway_name="FR"
        )
        # print(bar)
        barlist.append(bar)

    database_manager.save_bar_data(barlist)


def read_fr(symbol):
    start = datetime.strptime('20010901', "%Y%m%d")
    end = datetime.strptime('20210220', "%Y%m%d")

    read_barlist = database_manager.load_bar_data(
        symbol="ES",
        exchange=Exchange.CME,
        start=start,
        end=end,
        interval=Interval.HOUR
    )

    print(len(read_barlist))



if __name__ == "__main__":
    symbol = "ES"
    es_hour_file = "../FirstRateData/futures-active_1hour_4iey2/ES_continuous_adjusted_1hour.txt"
    es_min_file = "../FirstRateData/futures-active_1min_4zl13/ES_continuous_adjusted_1min.txt"
    load_fr(es_min_file, symbol)
    print("finish saving, start reading")
    # read_fr(symbol)