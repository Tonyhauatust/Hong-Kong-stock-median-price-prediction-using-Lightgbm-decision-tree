# Ticker close price data processing and prediction using Lightgbm 

In this repo I have used psycopg2 in Python to grab data from Postgresql and process the data using pandas. 
Then, I have used lightgbm ( a decision tree algorithm) to do regression for predicting next month's median close price. 
Best hyperparameter is searched by using hyperopt (parameters to be optimized is number of leaves and max depth for decision tree).

Accuracy score and important features can be viewed in lightgbm_modelling.ipynb.


# Note on Rogers-Satchell Volatility
