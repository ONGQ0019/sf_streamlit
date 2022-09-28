print('hi')


#import all necessary libraries
import pandas as pd
import sys, pickle
from datetime import datetime, date, timedelta 
from dateutil.relativedelta import relativedelta
import snowflake.connector
from datetime import datetime
from dateutil.parser import parse



ctx_ana = snowflake.connector.connect(
user = "ref_app",
password = 'PQ^ui@m123!rA|)o(CBD~',
account = "jd18085.southeast-asia.privatelink",
host = "jd18085.southeast-asia.privatelink.snowflakecomputing.com",
database = "db_dw_prod",
schema = "sc_ref",
role = "role_ref_app",
warehouse = "wh_analytics"
)



sql = '''
WITH TABLE2 AS (
WITH table1 AS (
SELECT t1.*, CASE WHEN landing_plan_price ='unk' THEN 100 ELSE landing_plan_price END AS landing_price FROM 
DB_ANALYTICS_PROD.SC_ANALYT.SUBSCR_SURVEY t1)
SELECT subscr_no,to_char(add_months(PORTOUT_DATE,-1) ,'YYYYMM') AS month_sqn FROM table1 
WHERE to_char(date(PORTOUT_DATE),'yyyymm') = '202207'
AND LANDING_price <= 15
AND subscr_no NOT IN (SELECT DISTINCT subscr_no
FROM DB_DW_PROD.SC_TB.COMBI_SUBSCR_INFO t1
where
cust_type = 'Consumer'
AND LINE_STATUS  IN ('Deleted')
and SUBSCR_TYPE IN ('IDR', 'U21', 'FRE', 'FRO')
AND ( (END_DATE IS NULL AND TO_DATE(START_DATE) <= LAST_DAY(add_months(CURRENT_DATE(),-3)))  
OR ( (TO_DATE(End_Date) > LAST_DAY(add_months(CURRENT_DATE(),-3)) and TO_DATE(End_Date)< '2022-08-01' ) AND TO_DATE(START_DATE) <= LAST_DAY(add_months(CURRENT_DATE(), -3))) )
and bp_group_2  IN  ('SIM Only','SIM only','SIM-only Plan' ,'HS contract','Plan with Device','Contract with Device','PWD')
AND ((portout_to_telco NOT IN ('M1','ST','SH') AND portout_to_telco IS NOT NULL AND line_status = 'Deleted') 
OR (portout_to_telco IS NULL AND line_status = 'Active'))
AND NETWORK_TYPE_DESC = 'Postpaid Mobile'))
SELECT table2.subscr_no , line_status, month_sqn,
case when bp_group_2 in ('SIM Only','SIM only','SIM-only Plan') then 'simo' else 'hs' end as simo_hs_group
FROM table2 LEFT JOIN  DB_DW_PROD.SC_TB.COMBI_SUBSCR_INFO t3 ON table2.subscr_no = t3.SUBSCR_NO 
WHERE 
LINE_STATUS  IN ('Deleted')
and SUBSCR_TYPE IN ('IDR', 'U21', 'FRE', 'FRO')
AND NETWORK_TYPE_DESC = 'Postpaid Mobile'
UNION 
SELECT DISTINCT subscr_no , line_status,  to_char(LAST_DAY(add_months(CURRENT_DATE(),-3)),'YYYYMM') AS month_sqn ,
case when bp_group_2 in ('SIM Only','SIM only','SIM-only Plan') then 'simo' else 'hs' end as simo_hs_group
FROM DB_DW_PROD.SC_TB.COMBI_SUBSCR_INFO t1
where
cust_type = 'Consumer'
AND LINE_STATUS  IN ('Active','Deleted')
and SUBSCR_TYPE IN ('IDR', 'U21', 'FRE', 'FRO')
AND ( (END_DATE IS NULL AND TO_DATE(START_DATE) <= LAST_DAY(add_months(CURRENT_DATE(),-3)))  
OR ( (TO_DATE(End_Date) > LAST_DAY(add_months(CURRENT_DATE(),-3)) and TO_DATE(End_Date)< '2022-08-01' ) AND TO_DATE(START_DATE) <= LAST_DAY(add_months(CURRENT_DATE(), -3))) )
and bp_group_2  IN  ('SIM Only','SIM only','SIM-only Plan' ,'HS contract','Plan with Device','Contract with Device','PWD')
AND ((portout_to_telco NOT IN ('M1','ST','SH') AND portout_to_telco IS NOT NULL AND line_status = 'Deleted') 
OR (portout_to_telco IS NULL AND line_status = 'Active'))
AND NETWORK_TYPE_DESC = 'Postpaid Mobile'
;

'''

df = pd.read_sql(sql,ctx_ana )
print(df.MONTH_SQN.head(1))
#trainingbase
