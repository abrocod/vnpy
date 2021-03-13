import csv
from datetime import datetime
from typing import List, Tuple

from vnpy.trader.database import BarOverview, DB_TZ
from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.rqdata import rqdata_client
from vnpy.trader.database import database_manager


APP_NAME = "DataManager"


class FRManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

    def import_from_csv_to_mongodb(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ):
        """ Import data from csv by calling mongodb_database engine. 
            This method is slow since it insert document one by one. 
        """
        fr_header_list = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
        es_df = pd.read_csv(file_path, 
                            header=None,
                            names=fr_header_list)

        # convert string time into timestamp format.
        # consider use python native datetime to avoid tz conversion error.
        es_df['DateTime'] =  pd.to_datetime(es_df['DateTime'])
        # es_df['DateTime'] = es_df['DateTime'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

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
            barlist.append(bar)
        print("Finish reading data, start saving to mongodb")

        database_manager.save_bar_data(barlist)
        print("Finish saving data to mongodb")


    def bulk_import_from_csv_to_mongodb(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ):
        """ Use pymongo instead of mongoengine - for bulk import from csv """
        fr_header_list = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
        es_df = pd.read_csv(file_path, 
                            header=None,
                            names=fr_header_list)

        # convert string time into timestamp format.
        # consider use python native datetime to avoid tz conversion error.
        es_df['DateTime'] =  pd.to_datetime(es_df['DateTime'])
        # es_df['DateTime'] = es_df['DateTime'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

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
            barlist.append(bar)
        print("Finish reading data, start saving to mongodb")

        database_manager.save_bar_data(barlist)
        print("Finish saving data to mongodb")


    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> List[BarOverview]:
        """"""
        return database_manager.get_bar_overview()

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bars = database_manager.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count = database_manager.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count

    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: str,
        start: datetime
    ) -> int:
        """
        Query bar data from RQData.
        """
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=datetime.now(DB_TZ)
        )

        vt_symbol = f"{symbol}.{exchange.value}"
        contract = self.main_engine.get_contract(vt_symbol)

        # If history data provided in gateway, then query
        if contract and contract.history_data:
            data = self.main_engine.query_history(
                req, contract.gateway_name
            )
        # Otherwise use RQData to query data
        else:
            if not rqdata_client.inited:
                rqdata_client.init()

            data = rqdata_client.query_history(req)

        if data:
            database_manager.save_bar_data(data)
            return(len(data))

        return 0

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime
    ) -> int:
        """
        Query tick data from RQData.
        """
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=datetime.now(DB_TZ)
        )

        if not rqdata_client.inited:
            rqdata_client.init()

        data = rqdata_client.query_tick_history(req)

        if data:
            database_manager.save_tick_data(data)
            return(len(data))

        return 0
