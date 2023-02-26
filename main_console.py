import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import etl as et
import transaction_customer as tc

USER="root"
PASSWORD="password"

spark = SparkSession.builder.master("local[*]").appName("Capstone").getOrCreate()


# customer_df,branch_df,credit_df=et.extract()
# transform_customer_df,transform_branch_df,transform_credit_df=et.transform(customer_df,branch_df,credit_df)
# et.branch_load(transform_branch_df,"capstone","CDW_SAPP_BRANCH",USER,PASSWORD)
# et.credit_load(transform_credit_df,"capstone","CDW_SAPP_CREDIT_CARD",USER,PASSWORD)
# et.customer_load(transform_customer_df,"capstone","CDW_SAPP_CUSTOMER",USER,PASSWORD)

def mainmenu():
    print("\n============================== Main Menu ===================================")
    print("\nPlease, Select From the Following Option ::")
    print("\n1. - ETL Process ")
    print("\n2. - View Transactions Details ")
    print("\n3. - View/Edit Cutomer Details ")
    print("\n4. - View API Status code ")
    print("\n5. - Visualization Menu ")
    print("\n0. - Exit ")
    choice=int(input("\nEnter Selection:: "))
    return choice

def m_main_tree(mchoice):
    while mchoice!=0:
        match mchoice:
            case 1:
                print("ETL")
                et.etl(spark)
                tc.load_data(spark,USER,PASSWORD)                      
                mchoice=mainmenu()
            case 2:
                tchoice=trans_menu()
                tran_tree(tchoice)
                mchoice=mainmenu()
            case 3:
                cchoice=cust_menu()
                cust_tree(cchoice)
                mchoice=mainmenu()
            case 4:
                print("API status")
                tc.api_status()
                mchoice=mainmenu()
            case 5:
                vchoice=viz_menu()
                vis_tree(vchoice)
                mchoice=mainmenu()
            case 0:
                return 0
            case _:
                print("Invalid choice, Please Select valid choice again...")
                mchoice=mainmenu()

def trans_menu():
    print("\n==============================Transaction Menu======================================")
    print("\nPlease, Select From the Following Option ::")
    print("\n1. Transactions made by customers living in a given zip code for a given month and year : ")
    print("\n2. Number and total values of transactions for a given type")
    print("\n3. Number and total values of transactions for branches in a given state")
    print("\n0. Return to Main Menu ")
    choice=int(input("\nEnter selection :: "))
    return choice

def tran_tree(tchoice):
    while tchoice !=0:
        match tchoice:
            case 1:
                print("\n=== Transactions made by customers living in a given zip code for a given month and year ===")
                tc.tranasaction_by_zip(spark)
                tchoice=trans_menu()
            case 2:
                print("\n=== Number and total values of transactions for a given type ===")
                tc.Transaction_by_type(spark)
                tchoice=trans_menu()
            case 3:
                print("\n=== Number and total values of transactions for branches in a given state ===")
                tc.transaction_by_state(spark)
                tchoice=trans_menu()
            case 0:
                return 0
            case _:
                print("Invalid choice.... Try again..")
                tchoice=trans_menu()

def cust_menu():
    print("\n ============================== Customer Menu ======================================")
    print("\nPlease, Selct From the Following Option ::")
    print("\n 1. Check the existing account details of a customer :")
    print("\n 2. Modify the existing account details of a customer :")
    print("\n 3. Generate a monthly bill for a credit card number for a given month and year :")
    print("\n 4. Display the transactions made by a customer between two dates")
    print("\n 0. Return to main menu ")
    choice=int(input("\nEnter Selection :: "))
    return choice

def cust_tree(cchoice):
    while cchoice !=0:
        match cchoice:
            case 1:
                print("\n===check the existing account details of a customer")
                tc.cust1_details(spark)
                cchoice=cust_menu()
            case 2:
                print("\n Inside cust menu 22")
                tc.modify_cust_details()
                cchoice=cust_menu()
            case 3:
                print("\n===Generate a monthly bill for a credit card number for a given month and year")
                tc.generate_monthly_bill(spark)
                cchoice=cust_menu()
            case 4:
                print("\n===Display the transactions made by a customer between two dates")
                tc.cust_two_dates(spark)
                cchoice = cust_menu()
            case 0:
                return 0
            case _:
                print("\n Invalid Choice..PLease select again..")
                cchoice=cust_menu()

def viz_menu():
    print("\n ============================== Visualization Menu ======================================")
    print("\n\n Please, Selct From the Following Option ::")
    print("\n 1. Which transaction type has a high rate of transactions :")
    print("\n 2. Which state has a high number of customers :")
    print("\n 3. The sum of all transactions for the top 10 customers :")
    print("\n 4. Percentage of applications approved for self-employed applicants :")
    print("\n 5. percentage of rejection for married male applicants :")
    print("\n 6. percentage of rejection for married male applicants :")
    print("\n 7. the top three months with the largest transaction data :")
    print("\n 8. Branch processed the highest total dollar value of healthcare transactions :")
    print("\n 0. Return to main menu ")
    choice=int(input("\nEnter Selection :: "))
    return choice


def vis_tree(vchoice):
    while vchoice!=0:
        match vchoice:
            case 1:
                print("Graph 1")
                vchoice=viz_menu()
            case 2:
                print("Graph 2")
                vchoice=viz_menu()
            case 3:
                print("Graph 3")
                vchoice=viz_menu()
            case 4:
                print("Graph 4")
                vchoice=viz_menu()
            case 5:
                print("Graph 5")
                vchoice=viz_menu()
            case 6:
                print("Graph 6")
                vchoice=viz_menu()
            case 7:
                print("Graph 7")
                vchoice=viz_menu()
            case 8:
                print("Graph 8")
                vchoice=viz_menu()
            case 0:
                return 0
            case _:
                print("Invalid choice, Please Select valid choice again..")
                vchoice=viz_menu()

m_choice=mainmenu()
print(m_choice)
m_main_tree(m_choice)

