#%%
from vnpy.app.cta_strategy.backtesting import BacktestingEngine, OptimizationSetting
from vnpy.app.cta_strategy.strategies.atr_rsi_strategy import (
    AtrRsiStrategy,
)
from vnpy.app.cta_strategy.strategies.king_keltner_strategy import (
    KingKeltnerStrategy,
)
from datetime import datetime

#%%
engine = BacktestingEngine()
engine.set_parameters(
    vt_symbol="ES.CME",
    interval="1m",
    start=datetime(2020, 1, 1),
    end=datetime(2021, 2, 20),
    rate=0.3/10000,
    slippage=0.2,
    size=300,
    pricetick=0.2,
    capital=1_000_000,
)
engine.add_strategy(AtrRsiStrategy, {})
# engine.add_strategy(KingKeltnerStrategy, {})


#%%
# run one backtest
engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
engine.calculate_statistics()
engine.show_chart()


# %%
# use optimizer to run multiple backtest
setting = OptimizationSetting()
setting.set_target("sharpe_ratio")
setting.add_parameter("atr_length", 3, 39, 5)
setting.add_parameter("atr_ma_length", 10, 30, 4)

# 遗传算法 - no multiprocess
# engine.run_ga_optimization(setting)

# 普通算法 - multiprocess
# engine.run_optimization(setting)

# %%
