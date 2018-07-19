import pandas as pd
import pyodbc
from sqlite3 import Error
import datetime
import numpy as np

pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

server ='150.0.0.98, 54937'
database = 'pcm' 
username = 'sa' 
password = 'sa@123' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("SELECT * FROM PERSONAL_MASTER")
tables = cursor.fetchall()

query = "SELECT * FROM PERSONAL_MASTER"
df = pd.read_sql(query, cnxn)

try:
    date = pd.read_sql("select date=CONVERT(VARCHAR(10),MAX(TRADE_DATE),103) from TRADE1_S WHERE CONVERT(VARCHAR(10),GETDATE(),103) !=CONVERT(VARCHAR(10),TRADE_DATE,103)",cnxn)
except:
    print("error")

try:    
    RMS_CM_PRICE = pd.read_sql("select TRDDATE,SYMBOL,MKTTYPE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,SHARE_TRADE,SHARE_VALUE from RMS_CM_PRICE", cnxn)
    RMS_CM_PRICE.columns = ['TRDDATE','SYMBOL','MKTTYPE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','SHARE_TRADE','SHARE_VALUE']

    if (RMS_CM_PRICE['TRDDATE'] == datetime.date.today()).any():
        RMS_CM_PRICE.to_csv('RMS_CM_PRICE.csv', sep=',')

    TEMP_NSE_BHAV = pd.read_sql("SELECT SYMBOL,mkttype,Close_price,Share_Trade,Share_value,getdate() FROM RMS_CM_PRICE", cnxn)
    TEMP_NSE_BHAV.columns= ["SCRIP_SYMBOL","Scrip_type","SCRIP_CLOSING_PRICE","TotTrdqty","TotTrdval","Timestamp"]

    TEMP_NSE_BHAV.loc[~TEMP_NSE_BHAV['Scrip_type'].isin(['EQ','BE']), 'SCRIP_SYMBOL'] = TEMP_NSE_BHAV.SCRIP_SYMBOL+'-'+TEMP_NSE_BHAV.Scrip_type
    TEMP_NSE_BHAV.to_csv('TEMP_NSE_BHAV.csv', sep=',')
    
    columns = ['exchange', 'SCRIP_SYMBOL', 'SCRIP_NAME', 'isin', 'Price', 'FILEDATE', 'ImportTime']
    LIVE_BackPrice = pd.DataFrame(columns=columns)

    a = TEMP_NSE_BHAV.loc[TEMP_NSE_BHAV.Scrip_type.isin(['EQ','BE','E2','E1','N1','P1','N2','N3','N4','N5'])]
    LIVE_BackPrice['SCRIP_SYMBOL'] = a['SCRIP_SYMBOL']
    LIVE_BackPrice['SCRIP_NAME'] = a['SCRIP_SYMBOL']
    LIVE_BackPrice['Price'] = a['SCRIP_CLOSING_PRICE']
    LIVE_BackPrice['FILEDATE'] = datetime.datetime.now()
    LIVE_BackPrice['ImportTime'] = datetime.datetime.now()
    LIVE_BackPrice['exchange'] = 'CASH'
    LIVE_BackPrice.to_csv('LIVE_BackPrice.csv', sep=',')

except:
    print("error")

try:
    tmopl = pd.read_sql("select EFF_POS_LIMIT, SCRIP_NAME from tmopl WHERE DATE1 IN ( SELECT MAX(DATE1)FROM TMOPL )", cnxn)
    tmopl.to_csv('tmopl.csv', sep=',')
except:
    print("Error")

try:
    tmfpl = pd.read_sql("select  EFF_FUT_LMT,SCRIP_NAME from tmfpl WHERE DATE1 IN ( SELECT MAX(DATE1)FROM tmfpl )", cnxn)
    tmfpl.to_csv('tmfpl.csv', sep=',')
except:
    print("error")

try:
    MWP1 = pd.read_sql("select QTY,SYMBOL from MWP1 WHERE DATE1 IN ( SELECT MAX(DATE1)FROM MWP1)", cnxn)
    MWP1.to_csv('MWP1.csv', sep=',')
except:
    print("error")

try:
    MWP = pd.read_sql("SELECT QTY,SYMBOL FROM MWP WHERE DATE1 IN ( SELECT MAX(DATE1)FROM MWP )", cnxn)
    MWP.to_csv('MWP.csv', sep=',')
except:
    print("error")

try:
    NPH1 = pd.read_sql("select QTY,SYMBOL from NPH1 WHERE DATE1 IN ( SELECT MAX(DATE1)FROM NPH1)", cnxn)
    NPH1.to_csv('NPH1.csv', sep=',')
except:
    print("error")

try:
    RMS_SCRIP_BSKT = pd.read_sql("SELECT app,EXCH,BSKTCODE,BSKTDESC,SCRIPTCODE,ACTUALHCUT FROM RMS_SCRIP_BSKT A,COMPANY_MASTER  B WHERE A.COCD = B.COMPANY_CODE",cnxn)
    RMS_SCRIP_BSKT.to_csv('RMS_SCRIP_BSKT.csv', sep=',')
except:
    print("error")

try:
    RMS_SEC_UNDER_BAN = pd.read_sql("select 'FNO', 'NSE', DATE, SCRIP_SYMBOL from RMS_SEC_UNDER_BAN WHERE CONVERT(VARCHAR(10),DATE,103) = CONVERT(VARCHAR(10),GETDATE(),103) ", cnxn)
    RMS_SEC_UNDER_BAN.columns=["APP","EXCHANGE","TRDDATE","SYMBOL"]
    RMS_SEC_UNDER_BAN.to_csv('RMS_SEC_UNDER_BAN.csv', sep=',')
    
    M_USER_SETTINGS = pd.read_sql("select * from M_USER_SETTINGS",cnxn)
    RESULT = ''
    if RESULT == '':
        RESULT = RMS_SEC_UNDER_BAN['SYMBOL']
    else:
        RESULT = RESULT + ',' + RMS_SEC_UNDER_BAN['SYMBOL']

    M_USER_SETTINGS['FOBandScrip'] = RESULT
    M_USER_SETTINGS.to_csv('M_USER_SETTINGS.csv', sep=',')
except:
    print("error")

VW_RMS_MRGN_INTRDAY = pd.read_sql("SELECT B.APP, B.EXCH, TRDDATE, TMCODE, TMSNAME, MTM_AMT, AMTM_AMT, TOT_MRGN, TOT_COLL, INTRA_FUNDING FROM VW_RMS_MRGN_INTRDAY a,COMPANY_MASTER B WHERE A.COCD = B.COMPANY_CODE AND CONVERT(VARCHAR(10),TRDDATE,103) = CONVERT(VARCHAR(10),GETDATE(),103) ",cnxn)
VW_RMS_MRGN_INTRDAY.columns=["APP", "EXCH", "TRDDATE", "TMCODE", "TMSNAME", "MTM_AMT", "AMTM_AMT", "TOT_MRGN", "TOT_COLL", "INTRA_FUNDING"]
VW_RMS_MRGN_INTRDAY.to_csv('VW_RMS_MRGN_INTRDAY.csv', sep=',')


try:
    RMS_GROUP_MAPPING = pd.read_sql("select GROUPCODE, FNO_NSE_TMCODE, FNO_NSE_MFTMCODE, FNO_BSE_TMCODE, FNO_BSE_MFTMCODE, FNO_MSX_TMCODE, FNO_MSX_MFTMCODE, CF_NSE_TMCODE, CF_NSE_MFTMCODE, F_MSX_TMCODE, CF_MSX_MFTMCODE, CF_USE_TMCODE, CF_USE_MFTMCODE, NCD_TMCODE, NCD_MFTMCODE, MCX_TMCODE, MCX_MFTMCODE from RMS_GROUP_MAPPING", cnxn)
    RMS_GROUP_MAPPING.to_csv('RMS_GROUP_MAPPING.csv', sep=',')
except:
    print("error")

try:
    RMS_HCUT_EXCH = pd.read_sql(" select APP, EXCH, TRDDATE, SYMBOL, SERIES, MRGN_RT, ISIN, CLOSE_PRICE FROM RMS_HCUT_EXCH WHERE TRDDATE in(select max(TRDDATE) from RMS_HCUT_EXCH)", cnxn)
    RMS_HCUT_EXCH.to_csv('RMS_HCUT_EXCH.csv', sep=',')
    
    RMS_HCUT = pd.read_sql("select TRDDATE, SYMBOL, SERIES, MRGN_RT from RMS_HCUT_EXCH", cnxn)
    RMS_HCUT.to_csv('RMS_HCUT.csv', sep=',')
except:
    print("error")

try:
    RMS_BRANCH = pd.read_sql("select BRANCH_CODE,BRANCH_NAME,NULL,NULL,NULL,NULL,NULL,NULL from BRANCH_MASTER", cnxn)
    RMS_BRANCH.columns=["FO_BRCH_CODE", "FO_BRCH_NAME", "FO_BRCH_PHONE1", "FO_BRCH_PHONE2", "FO_BRCH_CONTACT_PER1", "FO_BRCH_CONTACT_PER2", "FO_BRCH_EMAIL_ADDR","FO_BRCH_MOBILE"]
    RMS_BRANCH.to_csv('RMS_BRANCH.csv', sep=',')
except:
    print("error")

try:
    RMS_BOD_LIMIT_act = pd.read_sql("SELECT C.APP, C.EXCH, A.TRADEDATE, A.TMCODE, A.LIMIT FROM RMS_BOD_LIMIT A,(SELECT TMCODE,COCD,MAX(ROWID) ROWID FROM RMS_BOD_LIMIT GROUP BY COCD,TMCODE ) B,COMPANY_MASTER C WHERE A.ROWID = B.ROWID AND B.COCD = C.COMPANY_CODE SELECT d_1= SUM(LIMIT) FROM RMS_BOD_LIMIT_act",cnxn)
    RMS_BOD_LIMIT_act.columns=["APP", "EXCHANGE", "TRADEDATE", "TMCODE", "LIMIT"]
    RMS_BOD_LIMIT_act.to_csv('RMS_BOD_LIMIT_act.csv', sep=',')

    RMS_BOD_LIMIT = pd.read_sql("select * from RMS_BOD_LIMIT",cnxn)
    RMS_BOD_LIMIT.loc[(RMS_BOD_LIMIT['LIMIT'] == RMS_BOD_LIMIT_act['LIMIT']) & (RMS_BOD_LIMIT['COCD'] == RMS_BOD_LIMIT_act['EXCHANGE']) & (RMS_BOD_LIMIT['TMCODE'] == RMS_BOD_LIMIT_act['TMCODE']),'LIMIT'] = RMS_BOD_LIMIT_act['LIMIT']
    RMS_BOD_LIMIT.to_csv('RMS_BOD_LIMIT.csv', sep=',')
except:
    print("error")
# one insert and delete remain

try:
    
    RMS_ENTRY_DTLS = pd.read_sql("SELECT  APP,EXCH,TMCODE,TRDDATE,ADOCAMT,INTRAAMT FROM RMS_ENTRY_DTLS  A,COMPANY_MASTER B WHERE A.COCD = B.COMPANY_ADDRESS AND TRDDATE = getdate() AND ISNULL(Checker,'Y') ='Y'",cnxn)
    RMS_ENTRY_DTLS.to_csv('RMS_ENTRY_DTLS.csv', sep=',')

    d = sum(RMS_ENTRY_DTLS['ADOCAMT'])+sum(RMS_ENTRY_DTLS['INTRAAMT'])
    d

    # #RMS_ENTRY_DTLS remain  delete, update, insert, d1 remain
    #pd.read_sql("SELECT APP, EXCH, BRANCH, REGIONCODE, GROUPCODE, TMCODE, TRDDATE, ADOCAMT, INTRAAMT FROM '#RMS_ENTRY_DTLS' order by TMCODE",cnxn)
except:
    print("error")

try:    
    RMS_CASH_DAY_IN_OUT = pd.read_sql("SELECT *  FROM RMS_CASH_DAY_IN_OUT",cnxn)
    RMS_CASH_DAY_IN_OUT.columns=["APP","EXCH","TMCODE","AMT","COLlTYPE","transno","bgfdno"]
    RMS_CASH_DAY_IN_OUT.to_csv('RMS_CASH_DAY_IN_OUT.csv', sep=',')
except:
    print("error")

RMS_SC_DAY_IN_OUT = pd.read_sql("select [APP],[EXCH],[tmcode],SCRIPCODE,ISIN,[QTY],'' SUBTYPE,0 transno,0 SrNo from BEN_VIEW",cnxn)
RMS_SC_DAY_IN_OUT.columns = ['APP','EXCH','tmcode','SCRIPCODE','ISIN','QTY','SUBTYPE','transno','SrNo']
RMS_SC_DAY_IN_OUT.to_csv('RMS_SC_DAY_IN_OUT.csv', sep=',')

try:
    
    RMS_BOD_SEC = pd.read_sql("SELECT [APP],[EXCH],GETDATE(),[tmcode],MAX(SCRIPCODE),ISIN,SUM(QTY),max(Rate),max(HAIRCUT),sum(value) FROM BEN_VIEW_op WHERE CONVERT(DATETIME,CONVERT(VARCHAR(10),VOUCHER_DATE,103),103) < CONVERT(DATETIME, CONVERT(VARCHAR(10),GETDATE(),103) ,103) GROUP BY APP,EXCH,[TMCODE],[ISIN]",cnxn)
    RMS_BOD_SEC.columns = ["APP",'EXCHANGE','TRADEDATE','TMCODE','SCRIPCODE','ISIN','QTY','Rate','HAIRCUT','value']

    def f(row, tmcode, exchange):
        if len(row[tmcode])== 4 and row([exchange])=='NSE':
            row[tmcode] = '0'+row[tmcode]
        elif len(row[tmcode])== 3 and row([exchange])=='NSE':
            row[tmcode] = '00'+row[tmcode]
        elif len(row[tmcode])== 2 and row([exchange])=='NSE':
            row[tmcode] = '000'+row[tmcode]
        else:
            row[tmcode] = row[tmcode]
        return row[tmcode]

    RMS_BOD_SEC['TMCODE'] = RMS_BOD_SEC.apply(lambda row: f(row,'TMCODE', 'EXCHANGE') , axis=1)
    RMS_BOD_SEC.to_csv('RMS_BOD_SEC.csv', sep=',')
    
    LIVE_BEN_TRANSACTION = pd.DataFrame(columns=['COUNTER_CLIENT_ID','COLLATRAL','CLIENT_ID','ISIN','DEBIT_QUANTITY','NET_QUANTITY','ImportTime','HAIRCUT','VALUE','RATE'])
    LIVE_BEN_TRANSACTION['COUNTER_CLIENT_ID'] = RMS_BOD_SEC['EXCHANGE'] + np.where(RMS_BOD_SEC['APP']!='', '_'+RMS_BOD_SEC['APP'], '')
    LIVE_BEN_TRANSACTION['COLLATRAL'] = 'Y'
    LIVE_BEN_TRANSACTION['CLIENT_ID'] = RMS_BOD_SEC['TMCODE']
    LIVE_BEN_TRANSACTION['ISIN'] = RMS_BOD_SEC['ISIN']
    LIVE_BEN_TRANSACTION['DEBIT_QUANTITY'] = RMS_BOD_SEC['QTY']
    LIVE_BEN_TRANSACTION['NET_QUANTITY'] = -1*RMS_BOD_SEC['QTY']
    LIVE_BEN_TRANSACTION['ImportTime'] = datetime.datetime.now()
    LIVE_BEN_TRANSACTION['HAIRCUT'] = RMS_BOD_SEC['HAIRCUT']
    LIVE_BEN_TRANSACTION['VALUE'] = RMS_BOD_SEC['QTY']*(RMS_BOD_SEC['Rate'] -((RMS_BOD_SEC['Rate']*RMS_BOD_SEC['HAIRCUT'])/100))
    LIVE_BEN_TRANSACTION['RATE'] = RMS_BOD_SEC['Rate']
    
    LIVE_BEN_TRANSACTION.to_csv('LIVE_BEN_TRANSACTION.csv', sep=',')
except:
    print("error")

try:
    RMS_TM_DETAILS = pd.read_sql("SELECT [APP],[EXCHANGE],[Branch],[Regioncode],[groupcode],[TMCODE],[TMTYPE],[TMSNAME],[TMPANNO],[MOBILENO],[CONTACT1],[CONTACT2],[EMAIL],[BASKETCODE],CASSEC_RATIO,MTM,auto_trd,mrgn_val,coll_val FROM RMS_TM_DETAILS", cnxn)
    RMS_TM_DETAILS.columns = ['APP','EXCHANGE','Branch','Regioncode','groupcode','TMCODE','TMTYPE','TMSNAME','TMPANNO','MOBILENO','CONTACT1','CONTACT2','EMAIL','BASKETCODE','CASSEC_RATIO','MTM','auto_trd','mrgn_val','coll_val']

    def f(row, tmcode, exchange):
            if len(row[tmcode])== 4 and row([exchange])=='NSE':
                row[tmcode] = '0'+row[tmcode]
            elif len(row[tmcode])== 3 and row([exchange])=='NSE':
                row[tmcode] = '00'+row[tmcode]
            elif len(row[tmcode])== 2 and row([exchange])=='NSE':
                row[tmcode] = '000'+row[tmcode]
            else:
                row[tmcode] = row[tmcode]
            return row[tmcode]

    RMS_TM_DETAILS['TMCODE'] = RMS_TM_DETAILS.apply(lambda row: f(row,'TMCODE', 'EXCHANGE') , axis=1)
    RMS_TM_DETAILS.to_csv('RMS_TM_DETAILS.csv', sep=',')
    
    LIVE_CLIENT_MASTER = pd.DataFrame(columns=['COMPANY_CODE','CLIENT_ID','CLIENT_NAME','BRANCH_CODE','Regioncode','groupcode','CLIENT_NATURE'])
    if (RMS_TM_DETAILS['EXCHANGE'] == 'NSE').any() and (RMS_TM_DETAILS['APP'] == 'FNO').any() and (RMS_TM_DETAILS['TMCODE']!= None).any():


        LIVE_CLIENT_MASTER['COMPANY_CODE'] = RMS_TM_DETAILS['EXCHANGE'] + np.where(RMS_TM_DETAILS['APP']!='', '_', '')+ RMS_TM_DETAILS['APP']
        LIVE_CLIENT_MASTER['CLIENT_ID'] = RMS_TM_DETAILS['TMCODE']
        LIVE_CLIENT_MASTER['CLIENT_NAME'] = RMS_TM_DETAILS['TMSNAME']
        LIVE_CLIENT_MASTER['BRANCH_CODE'] = np.where(RMS_TM_DETAILS['Branch'].isnull(),'', RMS_TM_DETAILS['Branch'])
        LIVE_CLIENT_MASTER['Regioncode'] = np.where(RMS_TM_DETAILS['Regioncode'].isnull(),'', RMS_TM_DETAILS['Regioncode'])
        LIVE_CLIENT_MASTER['groupcode'] = np.where(RMS_TM_DETAILS['groupcode'].isnull(),'', RMS_TM_DETAILS['groupcode'])
        LIVE_CLIENT_MASTER['CLIENT_NATURE'] = np.where(RMS_TM_DETAILS['TMTYPE'].isnull(),'', RMS_TM_DETAILS['TMTYPE'])

    LIVE_CLIENT_MASTER.to_csv('LIVE_CLIENT_MASTER.csv', sep=',')
except:
    print("error")

try:
    RMS_BOD_MARGIN = pd.read_sql("SELECT APP,EXCH,GETDATE(),TMCODE,SUM(AMT ) [TOTCASH],SUM(CASE WHEN COLLTYPE='CASH' THEN AMT ELSE 0 END ) CASH,SUM(CASE WHEN COLLTYPE='BG' THEN AMT ELSE 0 END ) BG,SUM(CASE WHEN COLLTYPE='FD' THEN AMT ELSE 0 END ) FD FROM PCM.DBO.RMS_BOD_MARGIN GROUP BY APP,EXCH,TMCODE",cnxn)
    RMS_BOD_MARGIN.columns = ['APP','EXCHANGE','TRDDATE','TMCODE','TOTCASH','CASH','BG','FD']
    RMS_BOD_MARGIN.to_csv('RMS_BOD_MARGIN.csv', sep=',')
except:
    print("error")

try:
    CM_MASTER = pd.read_sql("SELECT APP,EXCH,A.Client_Id,CLIENT_ID2 FROM  CM_MASTER A,COMPANY_MASTER B WHERE A.COCD = B.COMPANY_CODE",cnxn)
    CM_MASTER.columns = ['APP','EXCH','Client_Id','CLIENT_ID2']
    CM_MASTER.to_csv('CM_MASTER.csv', sep=',')
except:
    print("error")

try:
    TRADE = pd.read_sql("SELECT COMPANY_CODE,TRADE_NUMBER,CLIENT_ID,REVERSE_CLIENT_ID,SCRIP_SYMBOL,SCRIP_NAME,TRADE_DATE,TRADE_DATETIME,EXPIRY_DATE,BUY_SALE,MKT_TYPE,QUANTITY,PRICE_PREMIUM,ORDER_NUMBER,ORDER_DATETIME,USER_ID,SETTLEMENT_NO,0 BILL_SETTLEMENT_NO,VALIDITY_FLAG,CUSTODIAN_CODE,CLIENT_ID2,BILL_DATE,INSTRUMENT_TYPE,OPTION_TYPE,STRIKE_PRICE,LOT_SIZE,'N' IS_MANUAL_BROKERAGE,Backoffice from LIVE_TRADE where Backoffice ='y'", cnxn)
    TRADE.columns = ['COMPANY_CODE','TRADE_NUMBER','CLIENT_ID','REVERSE_CLIENT_ID','SCRIP_SYMBOL','SCRIP_NAME','TRADE_DATE','TRADE_DATETIME','EXPIRY_DATE','BUY_SALE','MKT_TYPE','QUANTITY','PRICE_PREMIUM','ORDER_NUMBER','ORDER_DATETIME','USER_ID','SETTLEMENT_NO','BILL_SETTLEMENT_NO','VALIDITY_FLAG','CUSTODIAN_CODE','CLIENT_ID2','BILL_DATE','INSTRUMENT_TYPE','OPTION_TYPE','STRIKE_PRICE','LOT_SIZE','IS_MANUAL_BROKERAGE','Backoffice']

    TRADE['CLIENT_ID2'] = np.where(TRADE.CLIENT_ID2=='PROPRIETARY', TRADE['CLIENT_ID'], TRADE['CLIENT_ID2'])

    TRADE['CLIENT_ID2'] = np.where((TRADE.CLIENT_ID.isin(RMS_TM_DETAILS.loc[RMS_TM_DETAILS['TMTYPE'] == 'CP', 'TMCODE']))&(TRADE.CLIENT_ID != TRADE.CLIENT_ID2), TRADE['CLIENT_ID'], TRADE['CLIENT_ID2'])
    TRADE.head()

    if ((TRADE['Backoffice']=='Y').any() & (TRADE['PRICE_PREMIUM']>0).any() & (TRADE['INSTRUMENT_TYPE'].str.startswith('FUT')).any()):
        LIVE_BackPrice['exchange'].append(TRADE['COMPANY_CODE'], ignore_index=True)
        LIVE_BackPrice['SCRIP_SYMBOL'].append(TRADE['SCRIP_SYMBOL'], ignore_index=True)
        LIVE_BackPrice['SCRIP_NAME'].append(TRADE['SCRIP_NAME'], ignore_index=True)
    #     LIVE_BackPrice['isin'] = TRADE['ISIN'], ignore_index=True)
        LIVE_BackPrice['Price'].append(TRADE['PRICE_PREMIUM'], ignore_index=True)
        LIVE_BackPrice['FILEDATE'] = datetime.datetime.now()
        LIVE_BackPrice['ImportTime'] = datetime.datetime.now()
        LIVE_BackPrice['INSTRUMENTTYPE'] = TRADE['INSTRUMENT_TYPE']
    LIVE_BackPrice.head()

    TRADE.loc[((TRADE.Backoffice == 'Y') & (TRADE['INSTRUMENT_TYPE'].str.startswith('OPT'))),'PRICE_PREMIUM'] = 0
    # remain #livebackprice
    TRADE.to_csv('TRADE.csv', sep=',')
    LIVE_BackPrice.to_csv('LIVE_BackPrice.csv', sep=',')
except:
    print("error")


try:
    V = pd.read_sql("select APP, EXCH, TRDDATE, TMCODE, SPAN_MRGN, BUY_PREM, EXPOSURE from VW_RMS_MRGN", cnxn)
    if (V['TRDDATE'] == datetime.date.today()).any():
        V.to_csv('VW_RMS_MRGN.csv', sep=',')
    
    M = pd.read_sql("SELECT TRDDATE, TMCODE, BUY_PREM from VW_RMS_MRGN WHERE COMPANY_CODE = 'NSE_FNO' AND BUY_PREM !=0",cnxn)
    M.columns = ["MARGIN_DATE","CLIENT_ID","NET_BUY_PREMIUM"]
    if (V['TRDDATE'] == datetime.date.today()).any():
        M.to_csv('MG12_T1.csv', sep=',')

    M2 = pd.read_sql("SELECT TRDDATE, TMCODE, BUY_PREM from VW_RMS_MRGN WHERE COMPANY_CODE = 'NSE_FNO' AND BUY_PREM !=0",cnxn)
    M2.columns = ["MARGIN_DATE","CLIENT_ID","NET_BUY_PREMIUM"]
    if (V['TRDDATE'] == datetime.date.today()).any():
        M2.to_csv('MG12_T2.csv', sep=',')
except:
    print("error")

try:
    LIVE_Exposure = pd.read_sql("select sR_NO,SCRIP_CODE,Exposure_Price,GETDATE() from Exposure_Tab", cnxn)
    LIVE_Exposure.columns = ["Sr_No","Scrip_Code","Exposure_Price","Exposure_months"]
    LIVE_Exposure.append({'Sr_No':1, 'Scrip_Code':'NIFTY', 'Exposure_Price':3, 'Exposure_months':datetime.datetime.today()}, ignore_index=True)
    LIVE_Exposure.append({'Sr_No':2, 'Scrip_Code':'MINIFTY', 'Exposure_Price':3, 'Exposure_months':datetime.datetime.today()}, ignore_index=True)
    LIVE_Exposure.append({'Sr_No':3, 'Scrip_Code':'BANKNIFTY', 'Exposure_Price':3, 'Exposure_months':datetime.datetime.today()}, ignore_index=True)
    LIVE_Exposure.head()
    LIVE_Exposure.to_csv('LIVE_Exposure.csv', sep=',')
except:
    print("error")

try:
    v_penalty = pd.read_sql("SELECT APP, EXCHANGE, TRDDATE, TM_CODE, CLNT_CODE, SYMBOL, CONST_QTY, VIOLATION_QTY FROM VW_RMS_PENALTY A,COMPANY_MASTER B WHERE A.COCD = B.COMPANY_CODE",cnxn)
    if (v_penalty['TRDDATE'] == datetime.date.today()).any():
        VW_RMS_PENALTY.to_csv('VW_RMS_PENALTY.csv', sep=',')
except:
    print("error")

LIVE_BackPrice = pd.read_sql("SELECT A.COMPANY_CODE,SCRIP_SYMBOL,SCRIP_NAME,'',MTM_PRICE,GETDATE(),GETDATE(),INSTRUMENT_TYPE FROM PCM.DBO.MTMCLOSINGPRICE A,COMPANY_MASTER B WHERE A.COMPANY_CODE = B.COMPANY_CODE ",cnxn)
LIVE_BackPrice.to_csv('LIVE_BackPrice.csv', sep=',')

