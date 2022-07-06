"""
This code computes the IC of different factors and output results in csv files.
depends on Factor class setup in params.py
"""

import pandas as pd
import numpy as np
import scipy.stats
import mysql.connector
from mysql.connector import errorcode
from scipy.stats.mstats import winsorize
from params import config, query_date, query_status, Factor

query_holding = (
    """
    select total.InnerCode, total.delta, mk.HKStkMV as mktcap, t3.ClosePrice/t2.ClosePrice - 1 as ret1, 
    t4.ClosePrice/t2.ClosePrice - 1 as ret2,  t5.ClosePrice/t2.ClosePrice - 1 as ret3 from (
	select InnerCode, sum(change_amount) as delta from AlternativeData.ChangeHoldAmountSM
    where innerCode not in ({c}) and date between %s and  %s group by InnerCode having sum(change_amount) <>0 
    ) total
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday =  %s) t2
	on t2.InnerCode = total.InnerCode
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) t3
	on t3.InnerCode = total.InnerCode
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) t4
	on t4.InnerCode = total.InnerCode
    inner join (Select InnerCode, ClosePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) t5
	on t5.InnerCode = total.InnerCode
    inner join (Select InnerCode, HKStkMV from jydb.QT_HKDailyQuoteIndex where tradingday = %s) mk 
	on mk.InnerCode = total.InnerCode;
    """
)

try:
    cnx = mysql.connector.connect(**config)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    # get trading days and dataframe for extracting banned stock list
    tradeday_df = pd.read_sql(query_date, cnx)
    ban_df = pd.read_sql(query_status, cnx, params=[tradeday_df.date.max().strftime("%Y-%m-%d")])
    ban_df.start_date = pd.to_datetime(ban_df.start_date)
    ban_df.end_date = pd.to_datetime(ban_df.end_date)

    # start computing factor IC
    factor_names = ["delta", "neu", "absneu", "neuU", "neuD", "deltaU", "deltaD", "abstails", "absmiddle"]
    for n in [5, 10, 30]:
        factors = {}
        for name in factor_names:
            factors[name] = Factor(name)
        print("Processing {}-day inflow data".format(n))
        for i in range(tradeday_df.shape[0]):
            if n < i < tradeday_df.shape[0] - 30 and tradeday_df.at[i, "total"] >= 5:
                # get dates
                total1 = str(tradeday_df.at[i - n + 1, "date"])[:10]
                total2 = str(tradeday_df.at[i, "date"])[:10]
                t1 = str(tradeday_df.at[i - 2, "date"])[:10]
                t2 = total2
                t3 = str(tradeday_df.at[i + 5, "date"])[:10]
                t4 = str(tradeday_df.at[i + 10, "date"])[:10]
                t5 = str(tradeday_df.at[i + 30, "date"])[:10]
                mk = str(tradeday_df.at[i - 3, "date"])[:10]

                # get banned stock list
                if i <= 60:
                    ban_ls = ban_df.loc[ban_df.end_date <= tradeday_df.at[i + 60, "date"], 'code'].tolist()
                elif i >= tradeday_df.shape[0] - 60:
                    ban_ls = ban_df.loc[ban_df.start_date >= tradeday_df.at[i - 60, "date"], 'code'].tolist()
                else:
                    ban_ls = ban_df.loc[(ban_df.end_date <= tradeday_df.at[i + 60, "date"]) & (
                                ban_df.start_date >= tradeday_df.at[i - 60, "date"]), 'code'].tolist()

                if not ban_ls:
                    ban_ls = ["0"]

                # get data
                data_df = pd.read_sql(query_holding.format(c=','.join(['%s'] * len(ban_ls))), cnx,
                                      params=ban_ls + [total1, total2, t2, t3, t4, t5, mk])
                # c=','.join(['%s'] * len(ban_ls))

                # cleaning data
                after_win = winsorize(data_df.delta, limits=[0.025, 0.025])
                after_std = (after_win - np.mean(after_win)) / np.std(after_win)
                data_df["delta"] = after_std

                after_win = winsorize(data_df.mktcap, limits=[0.025, 0.025])
                data_df["mktcap_log"] = np.log(after_win)

                regre_re = scipy.stats.linregress(data_df["mktcap_log"], data_df["delta"])
                data_df["neu"] = data_df["delta"] - regre_re[1] - regre_re[0] * data_df["mktcap_log"]

                regre_re2 = scipy.stats.linregress(data_df["mktcap_log"], data_df["delta"].abs())
                data_df["absneu"] = data_df["delta"].abs() - regre_re2[1] - regre_re2[0] * data_df["mktcap_log"]
                data_df["absneu"] = data_df["absneu"] * np.where(data_df["delta"] > 0, 1, -1)

                data_df["ratio"] = data_df["delta"] / data_df["mktcap"]

                data_df1 = data_df.loc[data_df.delta > 0, ["ret1", "ret2", "ret3", "delta", "mktcap_log"]]
                data_df1.columns = ["ret1", "ret2", "ret3", "deltaU", "mktcap_log"]
                regre_re = scipy.stats.linregress(data_df1["mktcap_log"], data_df1["deltaU"])
                data_df1["neuU"] = data_df1["deltaU"] - regre_re[1] - regre_re[0] * data_df1["mktcap_log"]

                data_df2 = data_df.loc[data_df.delta < 0, ["ret1", "ret2", "ret3", "delta", "mktcap_log"]]
                data_df2.columns = ["ret1", "ret2", "ret3", "deltaD", "mktcap_log"]
                regre_re = scipy.stats.linregress(data_df2["mktcap_log"], data_df2["deltaD"])
                data_df2["neuD"] = data_df2["deltaD"] - regre_re[1] - regre_re[0] * data_df2["mktcap_log"]

                data_df["neu_rank"] = data_df["neu"].rank(pct=True)
                data_df["delta_rank"] = data_df["delta"].rank(pct=True)
                data_df["absneu_rank"] = data_df["absneu"].rank(pct=True)

                # if n == 30:
                #     print(data_df2.shape[0])

                for f in factors:
                    if f == "neuU":
                        factors[f].getIC(data_df1, f, t1)
                    elif f == "neuD":
                        factors[f].getIC(data_df2, f, t1)
                    elif f == "deltaU":
                        factors[f].getIC(data_df1, f, t1)
                    elif f == "deltaD":
                        factors[f].getIC(data_df2, f, t1)
                    elif f == "tails":
                        factors[f].getIC(data_df[(data_df.neu_rank<=0.1)|(data_df.neu_rank>=0.9)], "neu", t1)
                    elif f == "middle":
                        factors[f].getIC(data_df[(data_df.neu_rank>=0.1)&(data_df.neu_rank<=0.9)], "neu", t1)
                    elif f == "abstails":
                        factors[f].getIC(data_df[(data_df.absneu_rank <= 0.1) | (data_df.absneu_rank >= 0.9)], "neu", t1)
                    elif f == "absmiddle":
                        factors[f].getIC(data_df[(data_df.absneu_rank >= 0.1) & (data_df.absneu_rank <= 0.9)], "neu", t1)
                    else:
                        factors[f].getIC(data_df, f, t1)

            # output results
            for f in factors:
                factors[f].outputIC(n)

        print("Process finished.")
    cnx.close()
