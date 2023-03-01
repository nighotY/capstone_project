#import required libraries
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import pandas as pd
import re
import requests
import mysql.connector as mariadb
import datetime


#function to load data from database to spark dataframe
def load_data(spark,USER,PASSWORD):
        """function loads returns data from RDBMS to spark dataframe"""
#load branch data from creditcard_capstone.CDW_SAPP_BRANCH to spark dataframe
        load_branch=spark.read\
                        .format("jdbc")\
                        .options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=USER,\
                                    password=PASSWORD,\
                                    url="jdbc:mysql://localhost:3308/",\
                                    dbtable="creditcard_capstone.CDW_SAPP_BRANCH")\
                        .load()

#load creditcard data from creditcard_capstone.CDW_SAPP_CREDIT_CARD to spark dataframe
        load_credit=spark.read\
                        .format("jdbc")\
                        .options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=USER,\
                                    password=PASSWORD,\
                                    url="jdbc:mysql://localhost:3308/",\
                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD")\
                        .load()

#load customer data from creditcard_capstone.CDW_SAPP_CUSTOMER to spark dataframe
        load_customer=spark.read\
                        .format("jdbc")\
                        .options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=USER,\
                                    password=PASSWORD,\
                                    url="jdbc:mysql://localhost:3308/",\
                                    dbtable="creditcard_capstone.CDW_SAPP_CUSTOMER")\
                        .load()
    #   create temp vew of spark dataframe
        load_branch.createOrReplaceTempView('branch')
        load_credit.createOrReplaceTempView("credit_card")
        load_customer.createOrReplaceTempView("customer")
    #   return load_branch,load_credit,load_customer


#decorator to change funcionality of the print function
def Dprint(f):
    """decorator function to show spark datafrmae"""
    def inner(*arg,**kwarg):
        r=f(*arg,**kwarg)
        r.show()
        # return r
    return inner


#Function to get transaction by zipcode
def transaction_by_zipcode(spark,zipcode,year,month):
    """transaction by zip code"""
    data= spark.sql("SELECT c.CUST_ZIP, \
                      c.FIRST_NAME, \
                      c.LAST_NAME,\
                      c.LAST_UPDATED,\
                      cc.*\
                      FROM credit_card cc\
                      JOIN customer c ON  c.SSN=cc.CUST_SSN\
                      WHERE c.CUST_ZIP = '{}' AND YEAR(to_date(TIMEID,'yyyyMMdd')) = '{}' \
                      AND MONTH(to_date(TIMEID,'yyyyMMdd'))='{}'\
                      ORDER BY day(to_date(TIMEID,'yyyyMMdd')) DESC".format(zipcode,year,month))
    return data


#function to get zipcode user input
def input_zipcode():
    """user zip code input. Returns valid zipcode"""
    while True:
        zipcode=input("Enter zipcode using 4-5 digits :: ")
        zipcode=zipcode.strip()
        regex = re.compile(r'^(\d{4,5})$')
        if re.fullmatch(regex, zipcode):
            break
        else:
            print("\n Invalid zipcode entry...Try again..")
    return zipcode


#year input  
def input_year():
    """Year input. Returns valid year"""     
    while True:
        year=input("Enter year using 4 digits :: ")
        year=year.strip()
        if year.isdigit() and len(year)==4:
            break
        else:
            print("\n Invalid year entry...Try again :: ")
    return year

#month input
def input_month():
    """returns valid month"""
    while True:
        month=input("Enter month using 2 digits (example: March enter 03):: ")
        month=month.strip().lstrip('0')
        if month.isdigit():
            if int(month) in range (1,13):
                break
            else:
                print("\n Invalid month entry...Try again ..")
        else:
            print("\n Invalid month entry...Try again ..")
    return month

#function to get zipcode,year and month from user
def tran1_input():
    #zipcode input
    zipcode=input_zipcode()
    #year input       
    year=input_year()
    #month input
    month=input_month()
    return zipcode,year,month


@Dprint
def tranasaction_by_zip(spark):
      """Transaction by zipcode. returns sparkdata frame to decorator function"""
      #calling function tran1_input() to get user input for zipcode, year and month.
      #the return value of function is valid user input zipcode,year,month
      zipcode,year,month=tran1_input()
      print("\nTransactions made by customers living in zip code :{}, for a year :{}, and month :{} \
                \nOutput Order by day in descending order\n".format(zipcode,year,month))   
      tras_zip=transaction_by_zipcode(spark,zipcode,year,month)
      tras_zip=tras_zip.withColumn('CUST_SSN',concat(lit('XXXXX'),substring(col('CUST_SSN'),6,4)))
      tras_zip=tras_zip.withColumn('CUST_CC_NO',concat(lit('XXXXXXXXXXXX'),substring(col('CUST_CC_NO'),12,4))).drop(col('CUST_SSN'))
      tras_zip=tras_zip.withColumn('TIMEID',concat(substring(col('TIMEID'),1,4),lit("-"),substring(col('TIMEID'),5,2),lit("-"),\
                                                   substring(col('TIMEID'),7,2)))
      if tras_zip.isEmpty():
          print("No data found for given input...")
      return tras_zip


#Function 
@Dprint
def tran_no_total_by(spark,tr_type):
    return spark.sql("SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_ID) AS NUM_OF_TRANSACTION, \
                        SUM(TRANSACTION_VALUE) AS TOTAL_TRAN_VALUES FROM credit_card\
                        WHERE TRANSACTION_TYPE='{}'\
                        GROUP BY TRANSACTION_TYPE".format(tr_type))


#funtion to get user input for transaction type
def tran2_input(spark):
    print("Available Transaction Type")
    transaction_type=spark.sql("SELECT DISTINCT(TRANSACTION_TYPE) FROM credit_card")
    transaction_type.show()
    transaction_type= transaction_type.toPandas()
    transaction_type_list=pd.unique(transaction_type['TRANSACTION_TYPE'])
    while True:        
        tr_type=input("\nEnter Transaction Type : ")
        tr_type=tr_type.strip()
        if tr_type.title() in transaction_type_list:
            print("Valid Transaction Type ")
            break
        else:
            print("\nEntered Transaction type not found.. Try again..")
    return tr_type


#Function to get transaction values by transaction type
def Transaction_by_type(spark):
      tr_type=tran2_input(spark)
      tran_no_total_by(spark,tr_type)


# function display the number and total values of transactions for branches in a given state.
@Dprint
def tran_no_total_byst(spark,state):
    """Transaction total by state. Returns spark dataframe"""
    return spark.sql("SELECT cc.BRANCH_CODE AS BRANCH_CODE, \
                            COUNT(cc.TRANSACTION_ID) AS NO_OF_TRANSACTIONS, \
                            SUM(TRANSACTION_VALUE) AS TOTAL_VALUE \
                            FROM customer C \
                            LEFT JOIN credit_card cc on C.SSN = cc.CUST_SSN \
                            WHERE C.CUST_STATE = '{}'\
                            GROUP BY cc.BRANCH_CODE\
                            ORDER BY cc.BRANCH_CODE".format(state))


#function to get user input for state.
def tran3_input(spark):
    """Getting user input"""
    tr_state=spark.sql("SELECT DISTINCT(BRANCH_STATE) AS STATES FROM branch")
    tr_state= tr_state.toPandas()
    tr_state_list=pd.unique(tr_state['STATES'])
    print("Available transaction States for reference::\n{}".format(tr_state_list))
    while True:
        state=input("\nEnter State using 2 letters as shown in above reference list:: ")
        state=state.strip()
        if state.isalpha() and len(state) == 2:
            if state.upper() in tr_state_list:
                print("\nNumber and total value of transactions for branches in '{}' ::".format(state.upper()))
                break
            else:
                print("\nState is not found in list..Try again..")
        else:
            print("\nInvalid entry...Try again..")
    return state

#funcionn call to input for transaction by state
def transaction_by_state(spark):
      #calling using input. return type of this function is valid available state
      state=tran3_input(spark)
      #calling function to get the number and total values of transactions for branches in a given state.
      print("\n")
      tran_no_total_byst(spark,state)


#function to get customer details by using credit card
@Dprint
def cust_details_bycc(spark,name,lastname,cc_number,phone):
    """to get customer details using credit card. using spark query"""
    data= spark.sql("SELECT * FROM CUSTOMER \
               WHERE FIRST_NAME='{}'  AND LAST_NAME='{}' \
                     AND CREDIT_CARD_NO = '{}'\
                     AND CUST_PHONE = '{}' "\
                    .format(name,lastname,cc_number,phone))
    data=data.withColumn('SSN',concat(lit('XXXXX'),substring(col('SSN'),6,4)))
    data=data.withColumn('CREDIT_CARD_NO',concat(lit('XXXXXXXXXXXX'),substring(col('CREDIT_CARD_NO'),12,4)))
    if data.isEmpty():
          print("No data found for given input...")
    return data


#function to check the existing account details of a customer by ssn, first name and last name
@Dprint
def cust_details_byssn(spark,ssn,name,lastname):
    data= spark.sql("SELECT * FROM CUSTOMER \
               WHERE SSN = '{}' AND FIRST_NAME='{}' AND LAST_NAME='{}'" \
                    .format(ssn,name,lastname))
    data=data.withColumn('SSN',concat(lit('XXXXX'),substring(col('SSN'),6,4)))
    data=data.withColumn('CREDIT_CARD_NO',concat(lit('XXXXXXXXXXXX'),substring(col('CREDIT_CARD_NO'),12,4)))
    if data.isEmpty():
          print("No data found for given input...")
    return data


#ssn input
def input_ssn():
     while True:
          print("SSN Details")
          ssn = input("Enter ssn for customer using 9 digits:: ")
          ssn=ssn.replace('-','').replace(' ','')
          if ssn.isdigit() and len(ssn) == 9:
              break
          else:
               print("Invalid ssn.. Try again ..")
     return ssn


#function to get credit card number user input
def input_cc():
    while True:
        cc_number=input("Enter 16 digit credit card number :: ")
        if cc_number.isdigit() and len(cc_number) == 16:
            break
        else:
            print("invalid input  ..Try again ..")
    return cc_number


# phone number input
def input_phone_no():
     while True:
          phone=input("Input 10 digit phone number:: ")
          if phone.isdigit() and len(phone) == 10:
               break
          else:
               print("Invalid phone number..TryAgin...")
     phone=re.sub(r'(\d{3})(\d{3})(\d{4})',r'(\1)\2-\3',phone)
     # phone="("+phone[0:3]+')'+phone[3:6]+'-'+phone[6:10]
     return phone


#name input
def input_name(tp):
      """name input"""
      while True:
            name = input("Enter {} Name :: ".format(tp))
            name=name.strip()
            pattern=re.compile(r"^(\w+)$")
            if pattern.match(name):
               name=name.title()
               break
            else:
                  print("Invalid Input...Try again..")
      return name

    
#function get customer account details using ssn or credit card number
def cust1_details(spark):
     while True:
          print("\nchoose option to check account details details of customer by :: ")
          print("\n 1> Using SSN details :: ")
          print("\n 2> Using Credit card :: ")
          choice=input("\nEnter Selection :: ")
          if choice.isdigit() and int(choice) in range(1,3):
               print("\nchoice :{}".format(choice))
               break
          else:
               print("Invalid choice..Tryagain")          
    
     if choice == '1':
          print("===Customer Account Details by Using SSN===\n")
          ssn=input_ssn()
          name=input_name("First")
          last_name=input_name("Last")
          cust_details_byssn(spark,ssn,name,last_name)
     else:
          print("===Customer account details by using creditcard no===\n")
          cc_number=input_cc()
          name = input_name("First ")
          last_name=input_name("Last ")
          phone=input_phone_no()                              
          cust_details_bycc(spark,name,last_name,cc_number,phone)


#function to get custmer credit transaction details using credit card number
@Dprint
def get_cc(spark,cc_number):
    return spark.sql("SELECT * FROM credit_card \
              WHERE CUST_CC_NO = '{}'".format(cc_number))


#function to get credit card yearly transaction details 
@Dprint 
def get_cc_yr(spark,cc_number,year):
    return spark.sql("SELECT * FROM credit_card \
              WHERE CUST_CC_NO = '{}' AND YEAR(to_date(TIMEID,'yyyyMMdd')) = '{}' \
              ORDER BY MONTH(to_date(TIMEID,'yyyyMMdd')) DESC".format(cc_number,year))


#function to get credit card monthly transaction details 
@Dprint
def get_cc_mon(spark,cc_number,year,mon):
    data = spark.sql("SELECT * FROM credit_card \
              WHERE CUST_CC_NO = '{}' AND YEAR(to_date(TIMEID,'yyyyMMdd')) = '{}'\
              AND MONTH(to_date(TIMEID,'yyyyMMdd')) = '{}'"\
              .format(cc_number,year,mon))
    data = data.drop(col('CUST_SSN'))
    if data.isEmpty():
          print("No data found for given input...")
    return data


#function to get monthly expenses
@Dprint
def monthly_exp(spark,cc_number,month,yr):
    return spark.sql("SELECT SUM(TRANSACTION_VALUE) AS MONTHLY_BILL,cc.TRANSACTION_TYPE FROM credit_card cc\
           JOIN customer c ON cc.CUST_CC_NO=c.CREDIT_CARD_NO \
           WHERE cc.CUST_CC_NO = '{}' AND MONTH(to_date(TIMEID,'yyyyMMdd')) = '{}' \
           AND YEAR(to_date(TIMEID,'yyyyMMdd'))='{}'\
           GROUP BY cc.TRANSACTION_TYPE".format(cc_number,month,yr))


@Dprint
#function used to generate a monthly bill for a credit card number for a given month and year.
def monthly_bill(spark,cc_number,month,yr):
    return spark.sql("SELECT SUM(TRANSACTION_VALUE) AS MONTHLY_TOTAL FROM credit_card cc\
                    JOIN customer c ON cc.CUST_CC_NO=c.CREDIT_CARD_NO \
                    WHERE cc.CUST_CC_NO = '{}' AND MONTH(to_date(TIMEID,'yyyyMMdd')) = '{}' \
                    AND YEAR(to_date(TIMEID,'yyyyMMdd'))='{}'\
                    GROUP BY cc.CUST_CC_NO".format(cc_number,month,yr))


#function to generate monthly bill
def generate_monthly_bill(spark):
    #generate monthly bill
      print("Enter credit card number :: ")
      cc_number=input_cc()
      year=input_year()
      month=input_month()
      get_cc_mon(spark,cc_number,year,month)
      print("Monthly Expenses by Transation Type")
      monthly_exp(spark,cc_number,month,year)
      print("Monthly bill :: ")
      monthly_bill(spark,cc_number,month,year)


# function to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
@Dprint
def tran_bet_two_date(spark,cc_number,date1,date2):
    data= spark.sql("SELECT * FROM credit_card \
          JOIN customer ON customer.CREDIT_CARD_NO=credit_card.CUST_CC_NO\
          WHERE customer.CREDIT_CARD_NO='{}' AND to_date(TIMEID,'yyyyMMdd') >= to_date('{}','yyyyMMdd') AND \
          to_date(TIMEID,'yyyyMMdd') <= TO_date('{}','yyyyMMdd') \
          ORDER BY YEAR(to_date(TIMEID,'yyyyMMdd')) DESC, MONTH(to_date(TIMEID,'yyyyMMdd')) DESC, \
          DAY(to_date(TIMEID,'yyyyMMdd')) DESC" \
          .format(cc_number,date1,date2))
    data=data.withColumn('CUST_SSN',concat(lit('XXXXX'),substring(col('CUST_SSN'),6,4)))
    data=data.withColumn('CUST_CC_NO',concat(lit('XXXXXXXXXXXX'),substring(col('CUST_CC_NO'),12,4))) \
             .drop('SSN','CREDIT_CARD_NO','CUST_SSN','CUST_PHONE','CUST_EMAIL','LAST_UPDATED','CUST_COUNTRY')
    data=data.withColumn('TIMEID',concat(substring(col('TIMEID'),1,4),lit("-"),substring(col('TIMEID'),5,2),lit("-"),\
                                                   substring(col('TIMEID'),7,2)))
    if data.isEmpty():
          print("No data found for given input...")
    return data
    

#function to get user date input
def input_date(tp):
    """returns valid date in'yyyy/mm/dd format'"""
    while True:
        input_date=input("Enter the '{}' date in 'yyyy/mm/dd' format :: ".format(tp))
        pattern=re.compile(r"^(\d{4}\/\d{2}\/\d{2})$")
        if pattern.match(input_date):
            year,month,day=input_date.split('/')
            isvaliddate=True
            try:
                datetime.datetime(int(year),int(month),int(day))
            except ValueError:
                isvaliddate=False
            if(isvaliddate):
                break
            else:
                ("print invalid date")
        else:
            print("Invalid date...Try again..")
    return "".join(input_date.split("/"))


#function to get start and end date
def input_dates():
    while True:
        """returns valid start and end date"""
        date1=input_date("Start")
        date2=input_date("End")
        if int(date1)-int(date2) < 0:
            print("valid dates")
            break
        else:
            print("Improper Start and End Dates try again....")
    return date1,date2


# the transactions made by a customer between two dates. Order by year, month, and day in descending order
def cust_two_dates(spark):
      #the transactions made by a customer between two dates. Order by year, month, and day in descending order
      print("Enter credit card number :: ")
      cc_number=input_cc()
      date1,date2=input_dates()
      tran_bet_two_date(spark,cc_number,date1,date2)


#function to disply API endpoint response code
def api_status():
    url="https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    response=requests.get(url)
    print("API endpoint status code :-->>  {}".format(response.status_code))


#get the addres details from the customer
def input_address():
    print("\nNew Enter Address")
    print("Enter appartment Number")
    address={}
    while True:
        apt_no=input("Appartment No:: ")
        apt_no=apt_no.strip()
        if apt_no.isdigit() and int(apt_no) > 0:
            address['APT_NO']=apt_no
            break
        else:
            ("Invalid entry.. Try again")
    while True:
        street_name=input("Input street name : ")
        pattern=re.compile(r"^(\w+)\s?(\w+\s*)*$")
        if pattern.match(street_name):
            st=street_name.split(" ")
            if st[0][0].isdigit():
                street_name=st[0]+ " "+" ".join(st[1:]).title()
            else:
                street_name=street_name.title()
            address['STREET_NAME']=street_name
            break
        else:
            print("Invalid ")
    while True:
        city_name=input("Input City Name without any space : ")
        city_name=city_name.strip()
        pattern=re.compile(r"^(\w+)$")
        if pattern.match(city_name):
             address['CITY_NAME']=city_name.title()
             break
        else:
            print("invalid")

    while True:
        state=input("State (Abbreviation) e.x.(Alaska as 'AK') :: ")
        state=state.upper().strip()
        st_list=['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI'\
              ,'MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT'\
                ,'VT','VA','WA','WV','WI','WY']
        if state.isalpha() and len(state)==2 and state in st_list:
            address['CUST_STATE']=state
            break
        else:
            print("invalid input")

    address['CUST_ZIP']=int(input_zipcode())
    address['FULL_STREET_ADDRESS']=address['STREET_NAME']+','+str(address['APT_NO'])     
            
    return address


#update customer details in the RDBMS Table
def modify_cust_details(spark,USER,PASSWORD):
    try:        
        con = mariadb.connect(host = "127.0.0.1", 
        port = 3308,
        user = USER,
        password=PASSWORD,
        database="creditcard_capstone")
    except Exception as e:
        print(e)
    db_cursor=con.cursor()
    print("\nPlease Enter the SSN No Of The Customer You Wish to Modify :: ")
    ssn=input_ssn()
    name=input_name("First")
    last_name=input_name("Last")
    # query_alter="ALTER TABLE cdw_sapp_customer MODIFY COLUMN LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
    #              ON UPDATE CURRENT_TIMESTAMP"
    # db_cursor.execute(query_alter)
    query1="SELECT FIRST_NAME,MIDDLE_NAME,LAST_NAME,CREDIT_CARD_NO,FULL_STREET_ADDRESS,CUST_CITY,\
            CUST_STATE, CUST_COUNTRY,CUST_ZIP,CUST_PHONE,CUST_EMAIL,LAST_UPDATED \
            FROM cdw_sapp_customer WHERE SSN = '{}' and FIRST_NAME ='{}' \
            AND LAST_NAME='{}'".format(ssn,name,last_name)
    db_cursor.execute(query1)
    cr=db_cursor.fetchall()
    print("Original Record..\n")
    print(cr)
    if len(cr) == 0:
        print("\nOne or more inputs were invalid or record does not exist in the database for given ssn  ")
    else:
        address=input_address()
        print(address)
        update_address="UPDATE cdw_sapp_customer SET FULL_STREET_ADDRESS='{}', CUST_CITY='{}', \
                        CUST_STATE='{}', CUST_ZIP='{}' , LAST_UPDATED=now() WHERE SSN = '{}' AND FIRST_NAME= '{}' AND LAST_NAME='{}'"\
                        .format(address['FULL_STREET_ADDRESS'],address['CITY_NAME'],address['CUST_STATE'],address['CUST_ZIP']\
                                ,ssn,name,last_name)
        try:
            db_cursor.execute(update_address)
            con.commit()
            print("\nRecord Updated Successfully")
            print("Updated Record\n")
            db_cursor.execute(query1)
            cr=db_cursor.fetchall()
            print(cr)
        except Exception as e:
            print(e)    
        finally:
            db_cursor.close()
            con.close()
            load_data(spark,USER,PASSWORD)



      
      