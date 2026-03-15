# This is a trading bot.  
First of all we want to get some market data and do some signalling on the data.  
Using those signals we essentially decide if we want to make any trades.  

I plan on running this on an an EC2 instance, and probably store the data on S3. 
 
- Market_data: gets data from yahoo and maybe ibkr
- Risk: calculates risk
- Signals: calculates the signals for each ticker
- Backtest: runs back test 