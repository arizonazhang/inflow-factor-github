"""
this module defines the selector class that build inflow factor portfolios
selector class rebalances portfolios;
manager class computes portfolio return;
"""
import pandas as pd
import mysql.connector
import pymongo
import numpy as np
import scipy.stats
from sqlalchemy import create_engine
import time

portfolio_dict = {'Absneu Inflow Factor Portfolio (Long-only)': 3,
                  'Absneu Inflow Factor Portfolio': 2}


def print_with_time(str):
    """print string along with local time"""
    print(str, end="")
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f" Time: {t}")


def winsorize(array, sig):
    """
    winsorize the array given the significance:
    for values > mean + sig*std, values = mean + sig*std,
    for value < mean - sig*std, values = mean - sig*std
    """
    before_mean = np.mean(array)
    before_std = np.std(array)
    win = lambda x: before_mean - sig * before_std if x < before_mean - sig * before_std else (
        before_mean + sig * before_std if x > before_mean + sig * before_std else x)
    after_win = array.apply(win)
    return after_win


def standaradize(array):
    """standardize the array"""
    after_std = (array - np.mean(array)) / np.std(array)
    return after_std


def check_complete_records(start_date, end_date):
    """
    check if the records in ChangeAmountHoldSM is complete
    this is done by comparing unique dates in the table with unique dates in the jydb.LC_SHSZHSCHoldings table
    (tradingtype = 5, infosource=72 stands for hong kong stock connect trading info)
    """
    jy_records = """SELECT count(distinct EndDate) FROM jydb.LC_SHSZHSCHoldings
                 where tradingtype=5 and infosource=72 and EndDate between %s and %s and weekday(EndDate) <=4;"""
    alter_records = "SELECT count(distinct Date) FROM ChangeHoldAmountSM where Date between %s and %s;"
    no_jy = read_mysql(jy_records, start_date, end_date)
    no_alter = read_mysql(alter_records, start_date, end_date)
    no_jy = no_jy.values[0, 0]
    no_alter = no_alter.values[0, 0]
    print_with_time(f"Record check: {no_alter}/{no_jy} records covered.")

    # raise error if records are not complete
    if no_jy != no_alter:
        print_with_time("Not enough records in the jydb database! ")
        raise ValueError


def get_factor_data(ref_date):
    """
    get values of factors (pure, absneu, neu) for each individual stocks of the given two weeks ending on ref_date
    ref_date should be a string in the format of "%Y-%m-%d"
    """
    # get start date of the two week period ending on ref_date
    start_date = pd.to_datetime(ref_date).normalize() - np.timedelta64(13, "D")
    start_date = str(start_date)[:10]

    query_ban = """
    Select innercode from ChangeHoldAmountSM 
    group by innercode having min(date) >= 
    (Select distinct date from ChangeHoldAmountSM where date <= %s order by date desc limit 1 offset 59);
    """
    query_delta = """
    select InnerCode, sum(change_amount) as pure, min(Date) as start_date, max(Date) as date
    from AlternativeData.ChangeHoldAmountSM
    where date between %s and  %s group by InnerCode
    """
    query_last_trading = """
    select min(tradingday) from jydb.QT_HKDailyQuoteIndex where tradingday between %s and %s
    """
    query_mk = """
    SELECT p.InnerCode, p.ClosePrice, mk.HKStkMV as mktcap, m.SecuAbbr, m.ChiName, m.SecuCode FROM jydb.QT_HKBefRehDQuote p 
    left join (select * from jydb.QT_HKDailyQuoteIndex where tradingday = %s) mk on mk.InnerCode = p.InnerCode 
    left join (select * from jydb.HK_SecuMain) m on m.InnerCode = p.InnerCode
    where p.tradingday = %s;
    """

    # check for individual stocks that are listed in the previous 60 trading days
    ban_ls = read_mysql(query_ban, ref_date)
    ban_ls = ban_ls.innercode.astype(int).to_list()

    # check for the first trading day between start_date and ref_date
    trade_start = read_mysql(query_last_trading, start_date, ref_date)
    trade_start = str(trade_start.values[0, 0])[:10]

    # get sum of pure values of each individual stock
    factor = read_mysql(query_delta, trade_start, ref_date)
    # get market cap of each individual stock
    mk = read_mysql(query_mk, trade_start, trade_start)

    # merge market cap and factors
    factor.InnerCode = factor.InnerCode.astype(int)
    data = pd.merge(factor, mk, on=['InnerCode'], how='left')

    # exclude newly-listed stocks and small cap ones
    data = data[~data['InnerCode'].isin(ban_ls)]
    data = data[data['mktcap'] >= 5000000000]

    # check if inflow data is complete in database
    check_complete_records(trade_start, ref_date)

    # winsorize and standardization
    data["pure"] = winsorize(data["pure"], sig=3.5)
    data["pure"] = standaradize(data["pure"])
    data["mktcap"] = winsorize(data["mktcap"], sig=3.5)
    data["mktcap_log"] = np.log(data["mktcap"])

    # get neutralized factor
    slope, intercept, *_ = scipy.stats.linregress(data["mktcap_log"], data["pure"])
    data["neu"] = data["pure"] - intercept - slope * data["mktcap_log"]

    # get absolute neutralized factor
    slope, intercept, *_ = scipy.stats.linregress(data["mktcap_log"], data["pure"].abs())
    data["absneu"] = data["pure"].abs() - intercept - slope * data["mktcap_log"]
    data["absneu"] = data["absneu"] * np.where(data["pure"] > 0, 1, -1)

    return data


class Selector:
    """
    this class construct portfolios of the three strategies: pure, absneu and neu
    """
    win_rng = 3.5
    quantile = 0.05
    horizon = 14

    def __init__(self, name, date):
        self.name = name
        self.data = get_factor_data(date)

        # compute percentage rank of the given factor
        self.data['rank'] = self.data[self.name].rank(pct=True)

        # find stocks in long and short portfolio
        self.long = self.select_stocks(long=True)
        self.short = self.select_stocks(long=False)
        self.pos = pd.concat([self.long, self.short])

    def select_stocks(self, long):
        old_col = ["date", "InnerCode", "SecuAbbr", "SecuCode", self.name, "pure", "rank", "ChiName"]
        new_col = ["date", "code", "secuabbr", "ticker", "change_amount", "factor_value", "rank", "ChiName"]
        if long:  # long portfolio is comprised of stocks ranking top 5% in terms of the factor value
            pos = self.data.loc[self.data["rank"] >= 1 - self.quantile, old_col].sort_values("rank")
        else:    # short portfolio is comprised of stocks ranking bottom 5% in terms of the factor value
            pos = self.data.loc[self.data["rank"] <= self.quantile, old_col].sort_values("rank")
        pos.columns = new_col
        pos['recommendation'] = 'long' if long else 'short'
        pos['strategy'] = self.name

        # adjust date column: set to the week's friday and turn into string
        pos['date'] = pos['date'].apply(lambda x: x + np.timedelta64(4 - x.weekday(), 'D'))
        pos['date'] = pos['date'].dt.strftime("%Y-%m-%d")

        # round values
        pos['change_amount'] = pos['change_amount'].round(4)
        pos['factor_value'] = pos['factor_value'].round(4)
        return pos

    def upload_mysql(self):
        """upload portfolio into mysql table: InflowFactor"""
        query = """
        Insert into InflowFactor 
        (date,code,secuabbr,ticker,change_amount,factor_value,strategy,recommendation)
        values (%(date)s, %(code)s, %(secuabbr)s, %(ticker)s, %(change_amount)s, 
                            %(factor_value)s, %(strategy)s, %(recommendation)s)
        """
        replace_into_mysql(query, "InflowFactor", self.pos)

    def upload_mongo(self, long=True, short=True):
        """upload portfolio into mongo table: app_data.portfolio_detail"""

        # get top 10 components of long, short or long-short portfolio
        portfolio_name = f"{self.name.title()} Inflow Factor Portfolio"
        if long and short: # long-short portfolio: top 5 ranking stocks in long and bottom 5 ranking stocks in short
            pos = pd.concat([self.long.tail(5), self.short.head(5)])
            pos = pos.sort_values("rank")
            pos['weight'] = np.where(pos['recommendation']=='long', 1/self.pos.shape[0], -1/self.pos.shape[0])*100
            pos['weight'] = pos['weight'].round(2)
        elif long: # long portfolio: top 10 ranking stocks
            pos = self.long.tail(10).copy()
            pos['weight'] = round(100 / self.long.shape[0], 2)
            portfolio_name += " (Long-only)"
        elif short: # short portfolio: bottom 10 ranking stocks
            pos = self.short.head(10).copy()
            pos['weight'] = round(100 / self.short.shape[0], 2)

        try:
            # get portfolio id (as the assigned in mongo table), raise error if the portfolio is not in dictionary
            id = portfolio_dict[portfolio_name]

            # put data into a dictionary:
            data_dict = {'id': id, 'name': portfolio_name, 'last_rebalance_date': pos['date'].values[0]}
            constituent_dict = zip([f'constituent_{i}' for i in range(1, 11)], pos['ChiName'].to_list())
            weighting_dict = zip([f'weighting_{i}' for i in range(1, 11)], pos['weight'].to_list())
            data_dict.update(constituent_dict)
            data_dict.update(weighting_dict)

            # upload data
            insert_into_mongo(data_dict, 'portfolio_detail')
        except KeyError:
            print_with_time("This portfolio is not scheduled to be uploaded to mongo database.")


class Manager:
    """
    this class computes return of different strategies at different dates
    """
    long_cost = 0.2/100
    short_cost = 0.3/100

    def __init__(self, strategy, date):
        self.name = strategy  # 'pure', 'absneu' or 'neu'
        self.cal_date = date  # the date when the portfolio is traded, string format "%Y-%m-%d"
        self.last_reb_date = self.get_last_reb() # get the last rebalancing date
        self.perf = self.cal_return()

    def get_last_reb(self):
        """get the last rebalancing date before the calculation date"""
        df = read_mysql('select max(date) as date from AlternativeData.InflowFactor where date < %s;', self.cal_date)
        date = df.values[0, 0]
        return pd.to_datetime(date).strftime("%Y-%m-%d")

    def get_history(self):
        """
        get cumulative values at the last rebalancing date and at the last calculation date
        """
        query_history = """
        SELECT main.date, main.strategy, main.side as recommendation, main.cumulative_return as last_value,  
        case when i.initial_value is null then 1 else i.initial_value end as initial_value from InflowFactorReturn main
        left join (select strategy, side, cumulative_return as initial_value from InflowFactorReturn where date = %s) i 
        on i.strategy = main.strategy and i.side = main.side
        where date < %s and main.strategy = %s order by date desc limit 3;
        """
        df = read_mysql(query_history, self.last_reb_date, self.cal_date, self.name)
        return df

    def get_single_return(self):
        """
        get quotes of portfolio components at the last rebalancing date and at the calculation date
        """
        query_performance = """
        select s.strategy, s.recommendation, s.code, quote2.closePrice as end_price, quote1.closePrice as start_price from
        (select code, strategy, recommendation from InflowFactor where strategy = %s and date = %s) s
        inner join 
        (Select tradingday, InnerCode, closePrice  from jydb.QT_HKBefRehDQuote where tradingday = %s) quote1 on s.code = quote1.InnerCode
        inner join 
        (Select InnerCode, closePrice from jydb.QT_HKBefRehDQuote where tradingday = %s) quote2 on s.code = quote2.InnerCode;
        """
        df = read_mysql(query_performance, self.name, self.last_reb_date, self.last_reb_date, self.cal_date)
        # raise exception if no data is returned, i.e. the calculation date is not trading day
        if df.empty:
            print_with_time("Not a trading day!")
            raise ValueError

        df['gross_ret'] = df['end_price']/df['start_price']
        return df

    def cal_discount(self):
        """
        compute discount ratio of long, short and long-short portfolio to account for transaction cost
        """

        # get the second last rebalancing date
        query_2nd_last_reb = """select max(date) from InflowFactor where date < %s;"""
        snd_last_reb = read_mysql(query_2nd_last_reb, self.last_reb_date)
        snd_last_reb = snd_last_reb.values[0, 0]

        if not snd_last_reb: # if the last rebalancing date is the first rebalancing
            discount = pd.DataFrame()
            discount["recommendation"] = pd.Series(['long', 'short', 'long-short'])
            discount["d"] = pd.Series([1/(1+self.long_cost),
                                              1/(1+self.short_cost),
                                              1/(1+self.long_cost+self.short_cost)])
        else:
            query_old_pos = """
            select main.recommendation, main.code, quote2.closeprice as price_this_reb, 
                    quote1.closeprice as price_last_reb from InflowFactor main
            left join (select innercode, closeprice from jydb.QT_HKBefRehDQuote where tradingday = %s) 
                    quote1 on quote1.innercode = main.code
            left join (select innercode, closeprice from jydb.QT_HKBefRehDQuote where tradingday = %s) 
                    quote2 on quote2.innercode = main.code
            where main.date = %s and main.strategy = %s;
            """

            query_new_pos = """select recommendation, code from InflowFactor 
            where date = %s and strategy = %s;"""

            snd_last_reb = pd.to_datetime(snd_last_reb).strftime("%Y-%m-%d")
            # get positions of the second last rebalancing
            old_pos = read_mysql(query_old_pos, snd_last_reb, self.last_reb_date, self.last_reb_date, self.name)
            # get position of the this rebalancing
            new_pos = read_mysql(query_new_pos, self.last_reb_date, self.name)

            # please refer to the calculation mechanism in file
            old_pos['cum_ret'] = old_pos['price_this_reb'] / old_pos['price_last_reb']
            old_pos['cnt1'] = old_pos.groupby('recommendation').code.transform('count')
            old_pos['overall_ret'] = old_pos.groupby('recommendation').cum_ret.transform('sum')

            cnt2 = new_pos.groupby('recommendation').count().reset_index()
            cnt2.columns = ['recommendation', 'cnt2']
            old_pos = pd.merge(old_pos, cnt2, on='recommendation')

            old_pos['duplicate'] = np.where(old_pos.code.isin(new_pos.code), 1, 0)
            old_pos['x'] = old_pos['duplicate'] * np.minimum(1/old_pos['cnt2'], old_pos['cum_ret']/old_pos['overall_ret'])
            discount = old_pos.groupby('recommendation').x.sum().reset_index()
            discount.columns = ['recommendation', 'x']
            discount['r'] = np.where(discount['recommendation']=='long', self.long_cost, self.short_cost)
            discount['d'] = (discount['x']*discount['r']+1)/(1+discount['r'])
            ls_discount = (np.sum(discount['x']*discount['r']) + 1)/(1+self.long_cost+self.short_cost)
            discount.loc[len(discount.index)] = ['long-short', np.nan, np.nan, ls_discount]
        return discount

    def cal_return(self):
        perf = self.get_single_return()
        pre = self.get_history()
        dis = self.cal_discount()

        # calculate cumulative return of long-only and short-only portfolios starting from the previous rebalancing
        ret_portfolio = perf.groupby(["recommendation"]).gross_ret.mean().reset_index()
        ret_portfolio.columns = ["recommendation", "raw_ret"]
        ret_portfolio = pd.merge(ret_portfolio, dis, on=['recommendation'], how='outer')
        ret_portfolio["mul"] = np.where(ret_portfolio.recommendation == "long", 1, -1) * ret_portfolio["raw_ret"]
        # for long-short portfolio, cum_ret = long return - short return
        ret_portfolio.loc[ret_portfolio.recommendation == "long-short", "raw_ret"] = ret_portfolio['mul'].sum()
        ret_portfolio["cum_ret"] = ret_portfolio["raw_ret"] * ret_portfolio["d"]

        # get last_value and initial value
        if pre.empty:  # if the there is no calculation before this calculation date, start from value of 1
            ret_portfolio["last_value"] = 1
            ret_portfolio["initial_value"] = 1
        else:
            # merge dataframe to get previous cumulative values
            ret_portfolio = pd.merge(ret_portfolio, pre, on=['recommendation'])

        # compute cumulative value
        one_sided = np.where(ret_portfolio.recommendation != "long-short", True, False)
        # for long/short portoflio, cumulative_value = initial_value * cum_ret
        ret_portfolio.loc[one_sided, "cumulative_value"] = \
            ret_portfolio.loc[one_sided, "initial_value"] * ret_portfolio.loc[one_sided, "cum_ret"]
        # for long-short portfolio,
        # cumulative_value = initial_value * (1+long return - short return) = initial_value * cum_ret
        ret_portfolio.loc[~one_sided, "cumulative_value"] = \
            ret_portfolio.loc[~one_sided, "initial_value"] * (1 + ret_portfolio.loc[~one_sided, "cum_ret"])

        # compute daily return
        ret_portfolio["daily_ret"] = ret_portfolio["cumulative_value"] / ret_portfolio["last_value"] - 1
        ret_portfolio["date"] = self.cal_date
        ret_portfolio['strategy'] = self.name
        return ret_portfolio

    def upload_mysql(self):
        """upload return in InflowFactorReturn table"""
        insert_return_query = """
        REPLACE INTO InflowFactorReturn (`date`, `strategy`, `side`, `daily_return`, `cumulative_return`) 
            VALUES (%(date)s, %(strategy)s, %(recommendation)s, %(daily_ret)s, %(cumulative_value)s);
        """
        data = self.perf.copy()

        # round values
        data['daily_ret'] = data['daily_ret'].round(4)
        data['cumulative_value'] = data['cumulative_value'].round(4)

        replace_into_mysql(insert_return_query, "InflowFactorReturn", data)

    def upload_mongo(self, long=True, short=True):
        """upload values in mongodb table app_data.portfolio_performance"""
        portfolio_name = f"{self.name.title()} Inflow Factor Portfolio"
        if long and not short:
            portfolio_name += " (Long-only)"
            side = 'long'
        elif long and short:
            side = 'long-short'
        else:
            side = 'short'

        try:
            # get portfolio id (as the assigned in mongo table), raise error if the portfolio is not in dictionary
            id = portfolio_dict[portfolio_name]

            # put values into dictionary
            daily_ret = round(self.perf.loc[self.perf.recommendation == side, 'daily_ret'].values[0]*100, 4)
            cumulative = round(self.perf.loc[self.perf.recommendation == side, 'cumulative_value'].values[0], 4)
            data_dict = {'trading_date': self.cal_date, 'portfolio_id': float(id), 'portfolio_name': portfolio_name,
                         'daily_return': daily_ret, 'cumulative_value': cumulative}

            # upload data
            insert_into_mongo(data_dict, 'portfolio_performance')
        except KeyError:
            print_with_time("This portfolio is not scheduled to be uploaded to mongo database.")


def replace_into_mysql(query, table_name, data):
    """
    replace/insert data into the table (table_name) using the query
    """
    config = {
        'user': 'infoport',
        'password': 'HKaift-123',
        'host': '192.168.2.81',
        'database': 'AlternativeData',
        'raise_on_warnings': False
    }
    # connect to database
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    # upload data row by row
    cnt = 0
    for i in range(len(data.index)):
        data_query = data.iloc[i, :].to_dict()
        try:
            cursor.execute(query, data_query)
        except Exception as e:
            print(e)
            cnt += 1
    cnx.commit()
    cursor.close()
    cnx.close()

    # output results
    print_with_time(f"Uploaded {len(data.index)-cnt}/{len(data.index)} records into table [{table_name}]")


def insert_into_mongo(data_dict, coll_name):
    """insert values into mongodb"""
    myclient = pymongo.MongoClient("mongodb://app_developer:hkaift123@192.168.2.85:4010/")
    db = myclient["app_data"]
    coll = db[coll_name]
    try:
        res = coll.insert_one(data_dict)
        print_with_time(f'Uploaded 1 record into collocation [{coll_name}] successful. id: {res.inserted_id}')
    except Exception as e:
        print(e)


def read_mysql(query, *pars):
    """
    load data from mysql using the given query
    this function connects the AlternativeData database
    """
    engine = create_engine("mysql+mysqlconnector://infoport:HKaift-123@192.168.2.81/AlternativeData")
    data = pd.read_sql(query, engine, params=list(pars))
    return data