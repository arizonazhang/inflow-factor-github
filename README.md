## Background
Inflow factor portfolios are the factor portfolios based on southbound inflow. The factors are calculated as follows:
For each individual stock, the *pure*, *neu*, *absneu* factor are calculated as follows:
1. pure factor ("pure"): sum of the inflow amount of the last two weeks
2. neutralized factor ("neu")

To extract market cap influence, neutralized factor is calculated as follows:
$Y_t = \theta_0 + \theta_1 Ln(MV)_t + neu_t$ where $Y_t$ denotes the inflow amount at time $t$

3. absolute neutralized factor ("absneu")

This model is proposed to extract market value exposure from the absolute amount of the inflow amount. 
The flow direction (i.e. the signs) is then multiplied back to the residual. 
Intuitively, large-cap stocks should have both large inflow and outflow.

$|Y_t| = \beta_0 + \beta_1 Ln(MV_t) + e_t$

$absneu_t = e_t \frac{Y_t}{|Y_t|}$

We found that the factor values and stock return has a positive correlation when we look at the stocks with inflow ranked
in top 10% and bottom 10%. For the pure factor, the information coefficient (IC) reaches 4.59% (10-day average factor 
v.s. 10-day return). Therefore, stocks with extremely high inflow are likely to outperform stocks with extremely low inflow in
past 10 days. This relationship holds true even if we neutralize market cap of the inflow factor.

**description**: 

Stock connect southbound inflow is the amount of capital flowing from mainland investors to Hong Kong listed stocks. We regress the inflow values with log market capitalization, thus removing company size influence, obtaining the **absolute neutralized inflow ("absneu")**. Ranking individual stocks by their 2-week average absneu, We found that stocks with top 5% quantile (high inflow) are likely to outperform stocks with bottom 5% quantile (high outflow). 

(Long-only) We propose building a long portfolio of the stock with top 5% absneu (equally-weighted). The portfolio would be rebalanced every two weeks.

(Long-short) We propose building a portfolio that long stocks with top 5% absneu and short stocks with bottom 5% absneu (equally-weighted). The portfolio would be rebalanced every two weeks. In the past five years, this portoflio earned an annual return of more than 20% in the past 5 years with a Sharpe ratio of 1.7 after accounting for transaction cost. 


# Return Computation and Transaction Cost
## Transaction Cost
Transaction is accounted by multiplying stock return by a discount factor. 

### Long-only portfolio
We start by considering a long-only or short-only portfolio:

Suppose at $T=0$, we invest $M_0$ equally into $n_0$ stocks. 

At $T=1$, the each stock earned a cumulative return $r_i$ for $ i = 1, \dots, \ n_0$. 

So at $T=1$, the weighting of the $i$-th stock is $w_i = \frac{M_0(1+r_i)/n_0}{M_0\sum_{i=1}^{n_0}(1+r_i)/n_0} = \frac{(1+r_i)}{\sum_{i=1}^{n_0}(1+r_i)}$

The portfolio now worths $M_1$. 

Suppose we reinvest this amount equally into $n_1$ stocks, the portion of portfolio that could stay would be 

$x = \sum_{i=1}^{n_0} \mathbb{I}( \text{stock i stay}) \min(M_1/n_1, M_1 w_i)$

If all portfolio would be changed, only $\frac{M_1}{1+r_{cost}}$ could be used to buy stocks, so the return need to be discounted with a factor: $\frac{1}{1+r_{cost}}$

If some stocks could stay in the portfolio, an amount of $x\times r_{cost}$ could be saved to buy stocks. 

After accounting for transaction cost again, the additional value to be invested is $\frac{x r_{cost}}{1+r_{cost}}$. 

In total,  $\frac{M_1}{1+r_{cost}} + \frac{x r_{cost}}{1+r_{cost}}$ could be used to buy stocks.

So the **discount factor** is $D = \frac{1+x r_{cost}/M_1}{1+r_{cost}}$ where $x/M_1 = \sum_{i=1}^{n_0} \mathbb{I}( \text{stock i stay}) \min(1/n_1, w_i))$

### Long-short portfolio

at $T=1$, if both long and short side must be changed completely, only $\frac{M_1}{1+r_{L}+r_{S}}$, 

where $r_L$ and $r_S$ are the transaction cost of long and short side respectively.

Suppose the portion of long portfolio that could stay $x_L$ and the portion for short side is $x_S$. 

Then in total, $\frac{M_1}{1+r_{L}+r_{S}} + \frac{x_L r_L + x_S r_S}{1+r_{L}+r_{S}}$ so the **discount factor** is $D=\frac{1}{1+r_{L}+r_{S}} + \frac{x_L r_L / M_1 + x_S r_S / M_1}{1+r_{L}+r_{S}}$ 

## Daily Return and Cumulative Value
denote last rebalance date (nearest rebalance) as $t_0$ and calculation date $t$

$V_t$ denotes the cumulative value of the portfolio at $t$

$R_t$ denotes the daily return of the portfolio at the date $t$

$r_{t_0, t, i}$ denotes the simple return of stock i from $t_0$ to $t$

$D_t$ denotes the discount factor at $t$

**long-only, short-only portfolio**

$V_t = \frac{1}{n}\sum_{i=1}^{n}(1+r_{t_0, t, i})D_t V_{t_0}$

$R_t = V_t/V_{t-1}$

**long-short portfolio**

$V_t = (1 + \frac{1}{n_L}\sum_{i=1}^{n_L}(1+r_{t_0, t, i}^L)D_t - \frac{1}{n_S}\sum_{i=1}^{n_S}(1+r_{t_0, t, i}^S)D_t) V_{t_0}$ 

$R_t = V_t/V_{t-1}$

The reason to compute cumulative value before daily return is that weightings of the portfolio rebalances would change after the portfolio, i.e. not equally-weighted (due to stock price movements). Computing daily return directly is troublesome. Then computing the cumulative return starting from the last rebalancing date is more convenient and straight-forward. 

# Database
## MySQL: `InflowFactor`

| Column Name      | Description | Data Type | 
| ----------- | ----------- | ------ |
| date      | rebalancing date       | datetime (must be Friday)
| code   | jydb's ticker, i.e. `InnerCode`   |
| secuabbr | Company Name Abbreviation (linked to `HK_SecuMain`)| string|
| ticker | exchange ticker | |
|change_amount | sum of southbound inflow in the last 2 weeks | float, round to 4 digits|
| factor_value | value of the factor | float, round to 4 digits|
|strategy | 'pure', 'absneu' or 'neu' | |
|recommendation | 'long' or 'short' ||


## MySQL: `InflowFactorReturn`
| Column Name      | Description | Data Type | 
| ----------- | ----------- | ------ |
| date      | return calculation date      | datetime|
| strategy| 'pure', 'absneu' or 'neu' | |
| side | 'long-short', 'long' or 'short'| |
| daily_return | return of the calculation date | float, round to 4 digits|
| cumulative_value | cumulative value of the given strategy and side (calcualtion starts in 2022/03/04) | float, round to 4 digits|

Note: the table has a unique key for the columns combined: date, strategy, side

# Running the file

## Requirements:
- connection to database with read and write access (mysql, mongo)
- stock price data and inflow data updated

## Exceptions:
`select_stock`
- If data in ChangeHoldAmountSM is not complete, i.e. inflow data on some dates are not uploaded: `ValueError`
- If portfolio to be uploaded into mongodb is not included in `portfolio_dict`: `KeyError`

`compute_return`
- If the calculation date is not a trading day or trading of that day is not closed: `ValueError`
- If portfolio to be uploaded into mongodb is not included in `portfolio_dict`: `KeyError`

# Files
- portfolio rebalancing & return update: **main.py** and **inflow-factor-class.py**
- analysis & backtest (in the analysis folder): **correlation_test** (compute IC), **long_short_backtest.py** (backtest of the long short portfolio), **long_short_index_backtest.py** (backtest of the long top quantile and short index portfolio)
- report (summary of the analysis): **analysis\report.ipynb**
- mysql query (for table management): **h-detail-tables.sql** and **h-return-tables.sql**