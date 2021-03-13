
#%%
import pandas as pd
import numpy as np
from datetime import datetime

from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import database_manager
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
# from vnpy.app.data_manager.fr_data_engine import FRManagerEngine
from vnpy.app.data_manager.engine import ManagerEngine

#%%
main_eg = MainEngine()
event_eg = EventEngine()
fr_engine = FRManagerEngine(main_engine=main_eg, event_engine=event_eg)

symbol = "ES"
exchange = Exchange.CME
interval = Interval.HOUR



# %%
