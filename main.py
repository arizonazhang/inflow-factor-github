import pandas as pd
import numpy as np
from inflow_factor_class import Selector, Manager
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger


def select_stock(date):
    """Find portfolio stocks for the three strategies (rebalancing), date should be Friday"""
    print(f"Selecting portfolios for date: {date}")
    for strategy in ['pure', 'neu', 'absneu']:
        print(f"-- strategy: {strategy}")
        strat_pos = Selector(strategy, date)
        strat_pos.upload_mysql()
        if strategy == 'absneu':
            strat_pos.upload_mongo(long=True, short=True)
            strat_pos.upload_mongo(long=True, short=False)

    next_reb = pd.to_datetime(date) + np.timedelta64(14, "D")
    next_reb = str(next_reb)[:10]
    print(f"next rebalance date: {next_reb}")
    print("---------------------------------------")


def compute_return(date):
    print(f"Computing return for date: {date}")
    for strategy in ['pure', 'neu', 'absneu']:
        print(f"-- Strategy: {strategy}")
        try:
            manager = Manager(strategy, date)
            manager.upload_mysql()
            if strategy == 'absneu':
                manager.upload_mongo()
                manager.upload_mongo(short=False)
        except:
            print("Update return failed.")
    print("---------------------------------------")


def select_stock_yesterday():
    date = pd.to_datetime('today').normalize() - np.timedelta64(1, "D")
    date = date.strftime("%Y-%m-%d")
    select_stock(date)


def compute_return_yesterday():
    date = pd.to_datetime('today').normalize() - np.timedelta64(1, "D")
    date = date.strftime("%Y-%m-%d")
    compute_return(date)


if __name__ == "__main__":
    # dates = pd.date_range(start='2022-03-04', end='2022-07-05', freq='W-FRI')
    # for date in dates[::2]:
    #     date = str(date)[:10]
    #     select_stock(date)
    #
    # dates = pd.date_range(start='2022-03-07', end='2022-07-05', freq='B')
    # for date in dates:
    #     date = str(date)[:10]
    #     compute_return(date)

    # compute_return_yesterday()
    scheduler = BlockingScheduler()
    intervalTrigger1 = IntervalTrigger(weeks=2, start_date='2022-07-05 06:00:00')
    intervalTrigger2 = IntervalTrigger(days=1, start_date='2022-07-05 07:00:00')
    scheduler.add_job(select_stock_yesterday, intervalTrigger1, timezone='Asia/Shanghai', id="inflow_rebalancing")
    scheduler.add_job(compute_return_yesterday, intervalTrigger2, timezone='Asia/Shanghai', id="inflow_return_update")

    try:
        print("Scheduler starts...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
