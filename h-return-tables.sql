-- check tables
select * from InflowFactorReturn order by date desc;
select count(1) from InflowFactorReturn group by date order by date desc;
select * from app.portfolio_performance where portfolio_id > 2 order by trading_date desc;

-- delete records
Set sql_safe_updates = 0;
delete from InflowFactorReturn where date >= 20220301;
delete from app.portfolio_performance where trading_date > 20220318 and portfolio_id >=2;
Set sql_safe_updates = 1;

-- set unique columns
ALTER TABLE InflowFactorReturn ADD CONSTRAINT date_option UNIQUE (date, strategy, side);

-- 
select * from InflowFactorReturn where strategy = 'pure' and side = 'long-short' order by date desc;