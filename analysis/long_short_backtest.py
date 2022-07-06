"""
This code program builds backtest for long top x% of factors and short bottom x% of factors.
This scheme considers transaction cost in order to make long short exposure equivalent after cost deduction.
"""

from params import config, query_date, query_status, path, myWinsorize, Factor, getBanList
import pandas as pd
import numpy as np
import scipy.stats
import mysql.connector
from mysql.connector import errorcode
from scipy.stats.mstats import winsorize

query_holding = (
    """
    select t2.tradingday as date, s.SecuAbbr, total.InnerCode as Code, total.delta, mk.HKStkMV as mktcap, t3.ClosePrice/t2.ClosePrice - 1 as ret from (
	select InnerCode, sum(change_amount) as delta from AlternativeData.ChangeHoldAmountSM
    where innerCode not in ({c}) and date between %s and  %s group by InnerCode having abs(avg(change_amount)) > 0.0001
    ) total
    inner join (Select tradingday, InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday =  %s) t2
	on t2.InnerCode = total.InnerCode
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) t3
	on t3.InnerCode = total.InnerCode
    inner join (Select InnerCode, HKStkMV from jydb.QT_HKDailyQuoteIndex where tradingday = %s) mk 
	on mk.InnerCode = total.InnerCode
	left join jydb.HK_SecuMain s on s.InnerCode = total.InnerCode
	where mk.HKStkMV >= 5000000000 and t2.ClosePrice >= 1;
    """
)


query_holding = (
    """
    select t2.tradingday as date, s.SecuAbbr, total.InnerCode as Code, total.delta, mk.HKStkMV as mktcap, t3.ClosePrice/t2.ClosePrice - 1 as ret from (
	select InnerCode, sum(change_amount) as delta from AlternativeData.ChangeHoldAmountSM
    where innerCode not in ({c}) and date between %s and  %s group by InnerCode 
    ) total
    inner join (Select tradingday, InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday =  %s) t2
	on t2.InnerCode = total.InnerCode
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) t3
	on t3.InnerCode = total.InnerCode
    inner join (Select InnerCode, HKStkMV from jydb.QT_HKDailyQuoteIndex where tradingday = %s) mk 
	on mk.InnerCode = total.InnerCode
	left join jydb.HK_SecuMain s on s.InnerCode = total.InnerCode
	where mk.HKStkMV >= 5000000000;
    """
)


def winNstand(col, std=False):
    after_win = myWinsorize(data_df[col], sig=3.5)
    if std:
        after_win = (after_win - np.mean(after_win)) / np.std(after_win)
    return after_win


try:
    cnx = mysql.connector.connect(**config)
except:
    print("Connection to database failed.")
else:
    # get trading day and stock lists
    tradeday_df = pd.read_sql(query_date, cnx)
    ban_df = pd.read_sql(query_status, cnx, params=[tradeday_df.date.max().strftime("%Y-%m-%d")])
    ban_df.start_date = pd.to_datetime(ban_df.start_date)
    ban_df.end_date = pd.to_datetime(ban_df.end_date)

    # define storage variables
    d = {}
    M = {}
    out = {}
    err = 0
    strategy = ["delta", "neu", "absneu"]
    rate_long = 0.0015
    rate_short = 0.0025
    top = 0.95
    bottom = 0.05
    idx = 0

    # start sorting
    for n in [5, 10]:
        for m in [10]:
            for s in strategy:
                d[s+"_long"] = pd.DataFrame(columns=["Code", "amount", "ret"])
                d[s+"_short"] = pd.DataFrame(columns=["Code", "amount", "ret"])
                M[s] = 1
            for i in range(n + 1, tradeday_df.shape[0], m):
                if i < tradeday_df.shape[0] - m:
                    # get dates
                    total1 = str(tradeday_df.at[i - n + 1, "date"])[:10]
                    total2 = str(tradeday_df.at[i, "date"])[:10]
                    t2 = total2
                    t3 = str(tradeday_df.at[i + m, "date"])[:10]
                    mk = str(tradeday_df.at[i - n + 1, "date"])[:10] ## revised

                    # get banned stock list
                    ban_ls = getBanList(ban_df, tradeday_df, i)

                    # get data
                    data_df = pd.read_sql(query_holding.format(c=','.join(['%s'] * len(ban_ls))), cnx,
                                          params=ban_ls + [total1, total2, t2, t3, mk])

                    # cleaning data
                    data_df["delta"] = winNstand("delta", True)
                    data_df["mktcap"] = winNstand("mktcap")
                    data_df["mktcap_log"] = np.log(data_df["mktcap"])

                    # get neutralized factor
                    slope, intercept, *_ = scipy.stats.linregress(data_df["mktcap_log"], data_df["delta"])
                    data_df["neu"] = data_df["delta"] - intercept - slope * data_df["mktcap_log"]

                    # get absolute neutralized factor
                    slope, intercept, *_ = scipy.stats.linregress(data_df["mktcap_log"], data_df["delta"].abs())
                    data_df["absneu"] = data_df["delta"].abs() - intercept - slope * data_df["mktcap_log"]
                    data_df["absneu"] = data_df["absneu"] * np.where(data_df["delta"] > 0, 1, -1)

                    # compute ranking
                    data_df["neu_rank"] = data_df["neu"].rank(pct=True)
                    data_df["delta_rank"] = data_df["delta"].rank(pct=True)
                    data_df["absneu_rank"] = data_df["absneu"].rank(pct=True)
                    data_df["mkt_rank"] = data_df["mktcap"].rank(pct=True)

                    # DEBUG: data_df.loc[data_df[s+"_rank"] >= top, ["SecuAbbr", s+"_rank"]].sort_values(s+"_rank")

                    for s in strategy:
                        new_long = data_df.loc[data_df[s+"_rank"] >= top, ["Code", "ret"]]
                        new_short = data_df.loc[data_df[s+"_rank"] <= bottom, ["Code", "ret"]]
                        long_pos = d[s+"_long"]
                        short_pos = d[s+"_short"]

                        # update total capital
                        long_pos.amount = long_pos.amount * (1 + long_pos.ret)
                        short_pos.amount = short_pos.amount * (1 + short_pos.ret)

                        cap_long = long_pos.amount.sum()
                        cap_short = short_pos.amount.sum()
                        net_return = (cap_long - cap_short) / M[s]
                        net_return_long = cap_long / M[s] - 1
                        net_return_short = cap_short / M[s] - 1
                        M[s] += cap_long - cap_short

                        # update portfolio
                        remove_long = np.where(long_pos.Code.isin(new_long.Code), 0, 1).sum()
                        new_long["amount"] = M[s] / new_long.shape[0] / (1 + rate_long + rate_short)

                        remove_short = np.where(short_pos.Code.isin(new_short.Code), 0, 1).sum()
                        new_short["amount"] = M[s] / new_short.shape[0] / (1 + rate_long + rate_short)

                        # compute the amount of portfolio that remains in the portfolio
                        stay_long = np.where(long_pos.Code.isin(new_long.Code),
                                             np.minimum(long_pos.amount, M[s] / new_long.shape[0] / (1 + rate_long + rate_short)), 0).sum()
                        stay_short = np.where(short_pos.Code.isin(new_short.Code),
                                              np.minimum(short_pos.amount, M[s] / new_short.shape[0] / (1 + rate_long + rate_short)), 0).sum()

                        new_short["amount"] += (rate_long * stay_long + rate_short * stay_short) / (
                                    1 + rate_long + rate_short) / new_short.shape[0]
                        new_long["amount"] += (rate_long * stay_long + rate_short * stay_short) / (
                                    1 + rate_long + rate_short) / new_long.shape[0]

                        # compute avg market size level of long and short side
                        avgmkt_long = (data_df.loc[data_df[s+"_rank"] >= top, [
                            "mktcap"]].mean() - data_df.mktcap.mean()) / data_df.mktcap.std()
                        avgmkt_short = (data_df.loc[data_df[s+"_rank"] <= bottom, [
                            "mktcap"]].mean() - data_df.mktcap.mean()) / data_df.mktcap.std()

                        d[s+"_long"] = new_long
                        d[s+"_short"] = new_short

                        # save results
                        out[idx] = [t2, n, s, M[s], net_return, net_return_long, net_return_short, new_long.shape[0], new_short.shape[0],
                                 remove_long, remove_short, avgmkt_long[0], avgmkt_short[0]]
                        idx += 1

    # save portfolio return
    pd.DataFrame.from_dict(out, orient="index",
                           columns=["date", "n", "strategy", "M", "net_ret", "net_long", "net_short", "no_long", "no_short", "remove_long",
                                    "remove_short", "mktcap_long", "mktcap_short"]).to_csv(r".\long_short\return_v2_{:.0f}.csv".format(bottom*1000))

    cnx.close()
