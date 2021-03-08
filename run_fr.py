import pandas as pd
import numpy as np
from datetime import datetime

from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import database_manager
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.app.data_manager.fr_data_engine import FRManagerEngine

main_eg = MainEngine()
event_eg = EventEngine()
fr_engine = FRManagerEngine(main_engine=main_eg, event_engine=event_eg)

symbol = "ES"
exchange = Exchange.CME
interval = Interval.HOUR
# fr_engine.delete_bar_data(symbol=symbol, 
#     exchange=exchange, 
#     interval=interval)

symbol = "ES"
exchange = Exchange.CME
# interval = Interval.HOUR
interval = Interval.MINUTE

# es_hour_file = "../FirstRateData/futures-active_1hour_4iey2/ES_continuous_adjusted_1hour.txt"
es_min_file = "../FirstRateData/futures-active_1min_4zl13/ES_continuous_adjusted_1min_1yrs.txt"
    

fr_engine.import_fr_data_from_csv(file_path=es_min_file, symbol=symbol, 
                                interval=interval, exchange=exchange)



