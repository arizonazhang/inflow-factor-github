"""
This file stores some commonly-used variables for other files.
"""

config = {
    'user': 'infoport',
    'password': 'HKaift-123',
    'host': '192.168.2.81',
    # 'database': 'AlternativeData',
    'raise_on_warnings': False
}

# count the number of stocks traded by mainland investor on each trade day
query_date = (
    '''
    select date, sum(case when change_amount <> 0 then 1 else 0 end) as total from  AlternativeData.ChangeHoldAmountSM 
    group by date
    '''
)

query_status = (
    """
    Select code, min(date) as start_date, max(date) as end_date from AlternativeData.ChangeHoldAmountSM
    group by code
    having start_date != "2017-03-17" or end_date != %s;
    """
)

query_HSI = (
    """
    SELECT tradingday as date, PrevClosePrice as close_price, changePCT FROM jydb.QT_OSIndexQuote
where IndexCode = %s and tradingday >= 20170317;
    """
)

def myWinsorize(array, sig):
    import numpy as np
    array_mean = np.mean(array)
    array_std = np.std(array)
    win = lambda x: array_mean - sig * array_std if x < array_mean - sig * array_std else (
        array_mean + sig * array_std if x > array_mean + sig * array_std else x)
    afterWin = array.apply(win)
    return afterWin


path = r'C:\Users\arizonazhang\OneDrive - hkaift\research\capflow'

class Factor:
    def __init__(self, name):
        self.name = name
        self.values = {}
        self.sorts = []

    def getIC(self, df, colname, t):
        import scipy.stats
        r1, p1 = scipy.stats.spearmanr(df[["ret1", colname]])
        r2, p2 = scipy.stats.spearmanr(df[["ret2", colname]])
        r3, p3 = scipy.stats.spearmanr(df[["ret3", colname]])
        self.values[t] = [r1, p1, r2, p2, r3, p3]

    def getSortedReturn(self, df, n, m, addMktcap=False):
        grouping = lambda x, size=5: int(x * size) + 1 if x < 1 else size

        df["factor_rank"] = df[self.name].rank(pct=True)
        df["factor_rank"] = df["factor_rank"].apply(grouping)

        if addMktcap:
            df["mktcap_rank"] = df.mktcap.rank(pct=True)
            df["mktcap_rank"] = df["mktcap_rank"].apply(grouping, args=(3,))
        else:
            df["mktcap_rank"] = 0

        ls = [5, 10, 30]
        avg_df = df.groupby(['factor_rank', 'mktcap_rank', 'date'])["ret"].mean()
        avg_df = avg_df.to_frame().reset_index()
        avg_df["n"] = n
        avg_df["m"] = m
        self.sorts.append(avg_df)

    def outputIC(self, n):
        import pandas as pd
        pd.DataFrame.from_dict(self.values, orient="index",
                               columns=["corr_5", "p_5", "corr_10", "p_10", "corr_30", "p_30"]).to_csv(
            r'.\correlations\correlation_{}_{:d}d_v4.csv'.format(self.name, n))

    def outputRet(self):
        import pandas as pd
        loc = r".\portfolios\portfolio_sort_{}.csv".format(self.name)
        try:
            pd.concat(self.sorts, axis=0).to_csv(loc)
            print("File saved.")
        except IOError as e:
            if e == PermissionError:
                print("Please close destination file.")


def getBanList(ban_df, tradeday_df, i):
    if i <= 60:
        ban_ls = ban_df.loc[ban_df.end_date <= tradeday_df.at[i + 60, "date"], 'code'].tolist()
    elif i >= tradeday_df.shape[0] - 60:
        ban_ls = ban_df.loc[ban_df.start_date >= tradeday_df.at[i - 60, "date"], 'code'].tolist()
    else:
        ban_ls = ban_df.loc[(ban_df.end_date <= tradeday_df.at[i + 60, "date"]) & (
                ban_df.start_date >= tradeday_df.at[i - 60, "date"]), 'code'].tolist()
    if not ban_ls:
        ban_ls = ["0"]
    return ban_ls
