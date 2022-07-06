-- check tables
select * from InflowFactor order by date desc;
select * from app.portfolio_detail where id >= 2;
select * from app.inflow_portfolio order by date desc;

-- delete records
Set sql_safe_updates = 0;
delete from InflowFactor where date > 20220318;
delete from app.inflow_portfolio where date > 20200318;
Set sql_safe_updates = 1;

-- check source database
select distinct date from ChangeHoldAmountSM where date > 20220329 order by date desc;
select distinct tradingday from jydb.QT_HKBefRehDQuote where tradingday > 20220329 order by tradingday desc;
select * from jydb.QT_HKBefRehDQuote where tradingday = 20220407;