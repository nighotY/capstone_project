import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import requests

# load_dotenv()
# USER=os.environ.get('user')
# PASSWORD=os.environ.get('password')
USER="root"
PASSWORD="password"

#create spark session
# spark = SparkSession.builder.master("local[*]").appName("Capstone").getOrCreate()

def extract(spark):
    """extracts data from target file returns extracted data as spark dataframe """
        #load/read cdw_sapp_custmer.json file into spark dataframe 
    customer_df=spark.read.load("creditcard_system_data/cdw_sapp_custmer.json",format="json")
    #read/load cdw_sapp_branch.json file into spark branch dataframe
    branch_df=spark.read.load("creditcard_system_data/cdw_sapp_branch.json",format="json")
    #read/load cdw_sapp_credit.json file into pandas credit dataframe
    credit_df=spark.read.load("creditcard_system_data/cdw_sapp_credit.json",format="json").toPandas()
    #extract loan data from API end point
    url="https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    response=requests.get(url)
    loan_data=response.json()
    loan_data_df=spark.createDataFrame(loan_data)
    print("Data Extracted :::")
    return customer_df,branch_df,credit_df,loan_data_df


def transform(spark,customer_df,branch_df,credit_df):
    """transforms the data as per the mapping document and returns transformed dataframe"""
    #temporary table view of the custmer dataframe
    customer_df.createOrReplaceTempView("customer")
    #trnasform data as per mapping document
    transform_customer_df=spark.sql("SELECT CAST(SSN AS INT) AS SSN,\
                            CONCAT(UPPER(SUBSTR(FIRST_NAME,1,1)),LOWER(substr(FIRST_NAME,2))) AS FIRST_NAME, \
                            CONCAT(LOWER(MIDDLE_NAME)) MIDDLE_NAME, \
                            CONCAT(UPPER(substr(LAST_NAME,1,1)),LOWER(substr(LAST_NAME,2))) AS LAST_NAME, \
                            CREDIT_CARD_NO,\
                            CONCAT(STREET_NAME, ',' ,APT_NO) AS FULL_STREET_ADDRESS, \
                            CUST_CITY, \
                            CUST_STATE, \
                            CUST_COUNTRY,\
                            CAST(CUST_ZIP AS INT) AS CUST_ZIP, \
                            CONCAT('(',LEFT(RAND()*100+201,3),')',SUBSTR(CUST_PHONE,1,3),'-',SUBSTR(CUST_PHONE,4)) AS CUST_PHONE, \
                            CUST_EMAIL,\
                            CAST(LAST_UPDATED AS TIMESTAMP) AS LAST_UPDATED\
                            FROM customer")
    
    #transformed branch dataframe according to the mapping document
    transform_branch_df=branch_df.select(col("BRANCH_CODE").astype('int').alias('BRANCH_CODE'),\
                 col("BRANCH_NAME"),\
                 col("BRANCH_STREET"), \
                 col("BRANCH_CITY"),\
                 col("BRANCH_STATE"), \
                 when(col("BRANCH_ZIP").isNull(),'99999').otherwise(col("BRANCH_ZIP")).astype('int').alias('BRANCH_ZIP'),\
                 (concat( lit("(") ,substring(col("BRANCH_PHONE"),1,3),lit(")") ,substring(col("BRANCH_PHONE"),4,3), \
                          lit("-") ,substring(col("BRANCH_PHONE"),7,4) )).alias("BRANCH_PHONE"),\
                 to_timestamp(col("LAST_UPDATED")).alias("LAST_UPDATED")
                 )
    
    #transform credit dataframe according to mapping document
    credit_df['MONTH']=credit_df['MONTH'].astype(str).str.zfill(2)
    credit_df['DAY']=credit_df['DAY'].astype(str).str.zfill(2)
    credit_df["TIMEID"]=credit_df['YEAR'].astype(str)+ \
                    credit_df['MONTH'] + credit_df['DAY']
    credit_df.rename(columns={'CREDIT_CARD_NO':'CUST_CC_NO'},inplace=True)
    #selected columns to be added into transformed dataframe
    trans_credit_df=credit_df[['CUST_CC_NO','TIMEID','CUST_SSN','BRANCH_CODE','TRANSACTION_TYPE','TRANSACTION_VALUE','TRANSACTION_ID']]
    transform_credit_df=spark.createDataFrame(trans_credit_df)

    print("Data successfully transformed as per the mapping document::  ..")

    return transform_customer_df,transform_branch_df,transform_credit_df


def tables_exist(spark,database,tablename):
    """check if table already exist in target RDBMS database. returns boolean value"""
    db_table_schema=spark.read\
                        .format("jdbc")\
                        .option("url", "jdbc:mysql://localhost:3308/") \
                        .option("user", USER) \
                        .option("password", PASSWORD) \
                        .option("dbtable","information_schema.tables")\
                        .load()\
                        .filter("table_schema='{}'".format(database))
    res=db_table_schema.select('TABLE_NAME')
    return tablename.lower() in res.toPandas().values

def branch_load(spark,transform_branch_df,dbname,tablename,USER,PASSWORD):
    """loads the sparkdataframe to target RDBMS table"""
    if tables_exist(spark,dbname,tablename):
        print("{} table Has alreay Loaded".format(tablename))
    else:
        transform_branch_df.write \
                    .format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate","true") \
                    .option("url", "jdbc:mysql://localhost:3308/") \
                    .option("createTableColumnTypes", "BRANCH_CODE INT,BRANCH_NAME VARCHAR(25),BRANCH_STREET VARCHAR(50),\
                                                       BRANCH_CITY VARCHAR(25), BRANCH_STATE VARCHAR(10),BRANCH_ZIP INT,\
                                                       BRANCH_PHONE VARCHAR(25),LAST_UPDATED TIMESTAMP")\
                    .option("dbtable", "{}.{}".format(dbname,tablename)) \
                    .option("truncate","true") \
                    .option("user", USER) \
                    .option("password", PASSWORD) \
                    .option("characterEncoding","UTF-8") \
                    .option("useUnicode", "true") \
                    .save()
        print("{} Table created successfuly in RDBMS".format(tablename))
    
def credit_load(spark,transform_credit_df,dbname,tablename,USER,PASSWORD):
    """loads the sparkdataframe to target RDBMS table"""
    #check if table already exist in the database
    if tables_exist(spark,dbname,tablename):
        print("{} Table has been alreay Loaded".format(tablename))
    
    else:
        #load creditcard data from spark dataframe into reditcard_capstone.CDW_SAPP_CREDIT_CARD table in RDBMS
        transform_credit_df.write \
                    .format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate","true") \
                    .option("url", "jdbc:mysql://localhost:3308/") \
                    .option("createTableColumnTypes", "CUST_CC_NO VARCHAR(25),TIMEID VARCHAR(15),CUST_SSN INT,\
                                                       BRANCH_CODE INT, TRANSACTION_TYPE VARCHAR(25),TRANSACTION_VALUE DOUBLE,\
                                                       TRANSACTION_ID INT")\
                    .option("dbtable", "{}.{}".format(dbname,tablename)) \
                    .option("truncate","true") \
                    .option("user", USER) \
                    .option("password", PASSWORD) \
                    .option("characterEncoding","UTF-8") \
                    .option("useUnicode", "true") \
                    .save()
        print("{} Table created successfuly in RDBMS".format(tablename))


def customer_load(spark,transform_customer_df,dbname,tablename,USER,PASSWORD):
    """loads the sparkdataframe to target RDBMS table"""
    #check if table already exist in the database
    if tables_exist(spark,dbname,tablename):
        print("{} Table has been alreay Loaded".format(tablename))
    else:
        #load cUSTOMER data from spark dataframe into reditcard_capstone.CDW_SAPP_CREDIT_CARD table in RDBMS
        transform_customer_df.write \
                    .format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate","true") \
                    .option("url", "jdbc:mysql://localhost:3308/") \
                    .option("createTableColumnTypes", "SSN INT,FIRST_NAME VARCHAR(25),MIDDLE_NAME VARCHAR(25),LAST_NAME VARCHAR(25),\
                                                       CREDIT_CARD_NO VARCHAR(25),FULL_STREET_ADDRESS VARCHAR(50),CUST_CITY VARCHAR(25),\
                                                       CUST_STATE VARCHAR(25),CUST_COUNTRY VARCHAR(25),CUST_ZIP INT,CUST_PHONE VARCHAR(15),\
                                                       CUST_EMAIL VARCHAR(50),LAST_UPDATED TIMESTAMP")\
                    .option("dbtable", "{}.{}".format(dbname,tablename)) \
                    .option("truncate","true") \
                    .option("user", USER) \
                    .option("password", PASSWORD) \
                    .option("characterEncoding","UTF-8") \
                    .option("useUnicode", "true") \
                    .save()
        print("{} Table created successfuly in RDBMS".format(tablename))

def loan_load(spark,loan_data_df,dbname,tablename,USER,PASSWORD):
    """loads the sparkdataframe to target RDBMS table"""
    #check if table already exist in the database
    if tables_exist(spark,dbname,tablename):
        print("{} Table has been alreay Loaded".format(tablename))
    #load Loan data from spark dataframe into CDW_SAPP_loan_application table in traget RDBMS 
    else:
        loan_data_df.write \
                    .format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate","true") \
                    .option("url", "jdbc:mysql://localhost:3308/") \
                    .option("dbtable", "creditcard_capstone.{}".format(tablename)) \
                    .option("truncate","true") \
                    .option("user", USER) \
                    .option("password", PASSWORD) \
                    .option("characterEncoding","UTF-8") \
                    .option("useUnicode", "true") \
                    .save()
        print("{} Table created successfuly in RDBMS".format(tablename))


def etl(spark):
    customer_df,branch_df,credit_df,loan_data_df=extract(spark)
    transform_customer_df,transform_branch_df,transform_credit_df=transform(spark,customer_df,branch_df,credit_df)
    branch_load(spark,transform_branch_df,"creditcard_capstone","CDW_SAPP_BRANCH",USER,PASSWORD)
    credit_load(spark,transform_credit_df,"creditcard_capstone","CDW_SAPP_CREDIT_CARD",USER,PASSWORD)
    customer_load(spark,transform_customer_df,"creditcard_capstone","CDW_SAPP_CUSTOMER",USER,PASSWORD)
    loan_load(spark,loan_data_df,"creditcard_capstone","CDW_SAPP_loan_application",USER,PASSWORD)
    

