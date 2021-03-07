from vnpy.app.script_trader import init_cli_trading
from vnpy.app.script_trader.cli import process_log_event
from vnpy.gateway.ib import IbGateway
from time import sleep
from datetime import datetime
import pandas as pd
# 连接到服务器
setting = {
    "TWS地址": "127.0.0.1",
    "TWS端口": 7497,  # 4002, 7497
    "客户号":8 #每个链接用一个独立的链接号，一个IBAPI支持32个来同时链接
}
engine = init_cli_trading([IbGateway]) #返回Script_engine 示例，并且给main_engine注册了gateway
engine.connect_gateway(setting, "IB") #链接

# 查询资金 - 自动
sleep(10)
print("***查询资金和持仓***")
print(engine.get_all_accounts(use_df = True))
# 查询持仓
print(engine.get_all_positions(use_df = True))

# 订阅行情
from vnpy.trader.constant import Exchange
from vnpy.trader.object import SubscribeRequest
# 从我测试直接用Script_engine有问题，IB的品种太多，get_all_contracts命令不行,需要指定具体后才可以，这里使用main_engine订阅
req1 = SubscribeRequest("AAPL",Exchange.NASDAQ) #创建行情订阅
engine.main_engine.subscribe(req1,"IB")

print("***从IB读取历史数据, 返回历史数据输出到数据库和csv文件***")
from vnpy.trader.object import HistoryRequest
from vnpy.trader.object import Interval
start = datetime.strptime('20190901', "%Y%m%d")
end = datetime.strptime('20210220', "%Y%m%d")
historyreq = HistoryRequest(
   symbol="AAPL",
   exchange=Exchange.NASDAQ,
   start=start,
   interval=Interval.HOUR
)

# # 读取历史数据，并把历史数据BarData放入数据库
bardatalist = engine.main_engine.query_history(historyreq,"IB")

print("Jinchao run_ib.py: save data.")
# import pdb; pdb.set_trace();
from vnpy.trader.database import database_manager
database_manager.save_bar_data(bardatalist)

        # symbol: str,
        # exchange: Exchange,
        # interval: Interval,
        # start: datetime,
        # end: datetime
bardatalist1 = database_manager.load_bar_data(
    symbol="AAPL",
    exchange=Exchange.NASDAQ,
    start=start,
    end=end,
    interval=Interval.HOUR
)
print(bardatalist1)

# 把历史数据BarData输出到csv
# pd.DataFrame(bardatalist).to_csv("~/Trading/vn_data/csv_data/"+ str(historyreq.symbol) + ".csv" , index=True, header=True)
# print("History data export to CSV")

print("end")