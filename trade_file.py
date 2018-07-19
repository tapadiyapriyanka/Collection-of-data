import pandas as pd
import numpy as np
import time
import csv

pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

column_name = ['TradeNumber', 'TradeStatus', 'InstrumentType', 'Symbol', 'Expirydate', 'StrikePrice', 'Optiontype', 'Securityname', 'BookType', 'BookTypeName', 'MarketType', 'UserId', 'BranchNo', 'Buy_SellInd', 'TradeQty', 'Price', 'Pro_Client', 'Account', 'Participant', 'Open_CloseFlag', 'Cover_UncoverFlag', 'ActivityTime', 'LastModified_time', 'OrderNo.', 'OppositeBrokerId', 'OrderEntered_ModDateTime']

def printit():
	start = time.time()
	tradefo = pd.read_csv("TRADEFO.txt", sep=',', index_col=False, names=column_name,low_memory=False)
	tradefo.to_csv('TRADEFO.csv', sep=',')
	end = time.time()
	print("count of data in file = ", tradefo.TradeStatus.count())
	print("data written to file", end-start)

while True:
	printit()
