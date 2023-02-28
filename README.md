# capstone_project
## PER SCHOLAS(Data Engineering) Capstone Project
##  Project Requirement 
### Business Requirements - ETL 
#### 1. Functional Requirements - Load Credit Card Database (SQL)
#### * Req-1.1   
               Data Extraction and Transformation with Python and 
               PySpark
               a) For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following 
                  JSON files according to the specifications found in the mapping document. 
                   1. CDW_SAPP_BRANCH.JSON
                   2. CDW_SAPP_CREDITCARD.JSON
                   3. CDW_SAPP_CUSTOMER.JSON
               b) Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules 
                  to load data into RDBMS(SQL), perform the following:
                  b 1)Create a Database in SQL(MariaDB), named “creditcard_capstone.”
                  b 2) Create a Python and Pyspark Program to load/write the “Credit Card System Data” into 
                       RDBMS(creditcard_capstone).Tables should be created by the following names in RDBMS:
                      CDW_SAPP_BRANCH
                      CDW_SAPP_CREDIT_CARD
                      CDW_SAPP_CUSTOMER 
                      
#### Action/challanges Req 1) Data was extracted from JSON files, transformed according to the specifications and 
loaded to the MariaDB dababase. The tools used were Python 3, PySpark and SQL(MariaDB). One of the challenges was 
to finding the options to set into jdbc conncetivity to connect to the database with 'utf8mb3' format.
#### 2. Functional Requirements - Application Front-End   \ onOnce data is loaded into the database, we need a 
front-end (console) to see/display data. For that, create a console-based Python program to satisfy System 
Requirements 2 (2.1 and 2.2).                                               
#### * Req-2.1 
              Transaction Details Module :
              1)    Used to display the transactions made by customers living in a given zip code for a given month
                    and year.Order by day in descending order.
              2)    Used to display the number and total values of transactions for a given type.
              3)    Used to display the total number and total values of transactions for branches in a given state.
#### * Req-2.2
              1)    Used to check the existing account details of a customer.
              2)    Used to modify the existing account details of a customer.
              3)    Used to generate a monthly bill for a credit card number for a given month and year.
              4)    Used to display the transactions made by a customer between two dates. Order by year, 
                    month, and day in descending order.
#### Action/challanges Req 2) Data extracted from RDBMS tables and loaded in to pyspark dataframe. And used sql 
quries to Analyse data. Implmented console based menu driven program.
The tools were used Pyspark dataframe, SPARK sql, regex for user input validation, Numpy, mysql-connector.The 
challange faced while implementing req 2.1 & 2.2 was using mysql-connector to connect 'utf8mb3' database. 
But after reseach found out that mysql-connector8.0.30 has issues while connecting with 'utf8' Database. 
so insted used the mysql-connector 8.0.29
#### 3. Functional Requirements - Data analysis and Visualization.                                                                                                 After data is loaded into the database, users can make changes from the front end, and they can also 
view data from the front end. Now, the business analyst team wants to analyze and visualize the data 
according to the below requirements.
              1)    Find and plot which transaction type has a high rate of transactions.                
              2)    Find and plot which state has a high number of customers.
              3)    Find and plot the sum of all transactions for the top 10 customers, and which customer 
                    has the highest transaction amount.
#### Action/challanges Req 3)For the data Visualization of above requirements used the bar graph.
Used pandas dataframe and matplotlib visulization library of python. And th plots are
   1)    Find and plot which transaction type has a high rate of transactions.
![Screenshot_20230227_114615](https://user-images.githubusercontent.com/118306654/221757707-d1de770b-ed8c-4e17-82fb-71593e36bec6.png)
   2)    Find and plot which state has a high number of customers.
![Screenshot_20230227_114743](https://user-images.githubusercontent.com/118306654/221758072-babeea9f-cd82-4faf-9e87-641a4906cb72.png)
   3)    Find and plot the sum of all transactions for the top 10 customers, and which customer has the 
         highest transaction amount.
![Screenshot_20230227_114812](https://user-images.githubusercontent.com/118306654/221758283-05c7f95a-94d9-44d2-a4e5-2d249539acd5.png)

### LOAN application Data API 
Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. 
Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.

Banks want to automate the loan eligibility process (in real-time) based on customer details provided 
while filling out the online application form. These details are Gender, Marital Status, Education, Number of 
Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of 
identifying the customer segments to those who are eligible for loan amounts so that they can specifically 
target these customers. Here they have provided a partial dataset.

#### API Endpoint: [https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json)

The above URL allows you to access information about loan application information. This dataset 
has all of the required fields for a loan application. You can access data from a REST API by sending 
an HTTP request and processing the response.
#### 4. Functional Requirements
#### * Req-4
          a)     Create a Python program to GET (consume) data from the above API endpoint for the loan
                  application dataset.
          b)     Find the status code of the above API endpoint.
          c)     Once Python reads data from the API, utilize PySpark to load data into RDBMS(SQL).
                 The table name should be CDW-SAPP_loan_application in the database.
#### Action/challanges: Extracted data from API endpoint and displayed status code. Then 
loaded loan application data into target database table.Used python request library to extract
data from API endpoint. And Pyspark to load data into database.
#### 5. Functional Requirements - Data Analysis and Visualization for Loan Application
          Data Analysis and Visualization
          a)    Find and plot the percentage of applications approved for self-employed applicants.
          b)    Find the percentage of rejection for married male applicants.
          c)    Find and plot the top three months with the largest transaction data.
          d)    Find and plot which branch processed the highest total dollar value of
                 healthcare transactions.
                
#### Action/challanges:plots are as follows
a)    Find and plot the percentage of applications approved for self-employed applicants.
![Screenshot_20230227_114846](https://user-images.githubusercontent.com/118306654/221758491-104f9205-4861-4c0b-a909-acf3216f9e7d.png)

b)    Find the percentage of rejection for married male applicants. 
  ![image](https://user-images.githubusercontent.com/118306654/221752308-881a6f52-f415-4b58-b0a2-e7313daa901a.png)
  
c)    Find and plot the top three months with the largest transaction data.
![Screenshot_20230227_114940](https://user-images.githubusercontent.com/118306654/221758758-296bf3cd-b9e2-4ba1-8a51-abcfd3c6a2ec.png)
      d)      Find and plot which branch processed the highest total dollar value of healthcare
              transactions.                 
![Screenshot_20230227_115104](https://user-images.githubusercontent.com/118306654/221758971-c1afc0f3-23d0-4cc1-a1f8-153a74b2d843.png)
#### Tableau dashboard link: https://public.tableau.com/app/profile/yogi8280/viz/transactionTrends1/Dashboard1
####  Prerequisites:
software required to run the project. Install:
* 1 Pyspark
* 2 Java sdk 8.0
* 3 Python
####  Running the project:
* Fork/Clone the project from :https://github.com/nighotY/capstone_project.git
* cd capstone_project
* pip install -r reuirements.txt
* Run main module.py file
* Select the option menu 1 for ETL process.
####  Skillset/Tools used:
. Python \
. Pyspark \
. Pyspark SQL \
. Mysql-connector \
. Pandas \
. Numpy \
. Regex \
. MariaDB
