#import reruired libraries
import matplotlib.pyplot as plt 
import pandas as pd
import random
import numpy as np


#function to get random colors for the plots
def colors(num):
    """returns list of random hex code generated """
    return ["#"+''.join([random.choice('ABCDEF012345678') for j in range(6)]) for i in range (num)]


#Function to load data from RDBMS to spark dataframe
def load_data_df(spark,USER,PASSWORD,tablename):
        """function loads returns data from RDBMS to spark dataframe"""
#load branch data from creditcard_capstone.CDW_SAPP_BRANCH to spark dataframe
        res_df=spark.read\
                        .format("jdbc")\
                        .options(driver="com.mysql.cj.jdbc.Driver",\
                                    user=USER,\
                                    password=PASSWORD,\
                                    url="jdbc:mysql://localhost:3308/",\
                                    dbtable="creditcard_capstone.{}".format(tablename))\
                        .load().toPandas()
        return res_df


#function to plot transaction value per category
def plot_tran_per_category(spark,USER,PASSWORD):
    """plot transaction per category. using matplotlib.pyplot library"""
    #tansactions value count by categories
    cc_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CREDIT_CARD")
    tran_count=cc_df['TRANSACTION_TYPE'].value_counts()
    tran_count=tran_count.sort_values()
    tr_colors=colors(len(tran_count))
    tran_count.plot(kind ='barh', figsize=(10,5),xlim=(6000,7000),color=tr_colors)
    plt.title("Total Transactions Per Category")
    for index,value in enumerate(tran_count):
        plt.text(value+10,index-0.1,str(value))
    plt.xlabel("Total Transactions")
    plt.ylabel("Categories")
    plt.show()


#Function to plot and find state which has high number of cutomer
def plot_total_cust_per_state(spark,USER,PASSWORD):
    """plot toltal customer per state"""
    cust_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CUSTOMER")
    states=cust_df['CUST_STATE'].value_counts()
    states=states.sort_values()
    st_colors=colors(len(states))
    states.plot(kind ='barh', figsize=(10,5),xlim=(0,100),color=st_colors)
    plt.title("Total Number of Customers Per State")
    for index,value in enumerate(states):
        plt.text(value+.5,index-0.1,str(value))
    plt.xlabel("Customers")
    plt.ylabel("States")
    xtick=[(i+5) for i in range(101)]
    plt.xticks([i for i in range(0,states.max()+5,5)])
    plt.annotate('State With \nHighest Number\nOf Customers', 
                    xy=(96,25),            
                    xytext=(80,22),  
                    arrowprops=dict(facecolor='black',width=1,headwidth=5),
                    horizontalalignment='left', 
                    verticalalignment='top')
    plt.show()


#function to plot top 10 customers
def plot_top_10_cust(spark,USER,PASSWORD):
    """plot top 10 customers"""
    cust_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CUSTOMER")
    cc_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CREDIT_CARD")
    #inner join on custmer dataframe and credit card dataframe
    cust_cc_df=pd.merge(cust_df,cc_df,how='inner',left_on='CREDIT_CARD_NO',right_on='CUST_CC_NO')
    top_10=cust_cc_df.groupby('CUST_SSN')['TRANSACTION_VALUE'] \
                    .sum() \
                    .sort_values().tail(10)
    top_10_colors=colors(len(top_10))
    top_10.plot(kind ='barh',figsize=(10,5),xlim=(4800,5800),color=top_10_colors)
    plt.title("Top 10 Total Transaction Amount Per Customer")
    for index,value in enumerate(top_10):
        plt.text(value+.20,index-0.2,'$'+str(value))
    plt.xlabel("Total Transaction value ")
    plt.ylabel("Customer Number")
    plt.annotate('Customer With \nHighest Transaction \n Amount', 
                xy=(5700,8.5),            
                xytext=(5600,6),  
                arrowprops=dict(facecolor='black',width=1,headwidth=5),
                horizontalalignment='left', 
                verticalalignment='center')
    plt.show()


# Function to plot the percentage of applications approved for self-employed applicants
def plot_self_emp(spark,USER,PASSWORD):
    """plot the percentage of application approved for self employed"""
    loan_df=load_data_df(spark,USER,PASSWORD,"cdw_sapp_loan_application")
    app_data=loan_df[['Application_Status','Self_Employed']].value_counts()
    values=app_data.values.tolist()
    status=[i[0] for i in app_data.index.values]
    fig, ax = plt.subplots(figsize=(15, 8), subplot_kw=dict(aspect="equal"))
    def func(pct, allvals):
        absolute = int(np.round(pct/100.*np.sum(allvals)))
        return f"{pct:.2f}%\n({absolute:d})"
    wedges, texts, autotexts = ax.pie(values, autopct=lambda pct: func(pct, values),
                                    textprops=dict(color="w"),explode=(0,0,0.1,0))
    legend_lables=['Approved Not Self-Employed','Not-Approved Not Self-Employed',\
                   'Approved Self-Employed','Not Approved Self-Employed']
    ax.legend(wedges, status, \
            title="Application-Status", \
            loc="upper right", \
            bbox_to_anchor=(1, 0, 0.5, 1),labels=legend_lables)

    plt.setp(autotexts, size=8, weight="bold")

    ax.set_title("Percentage of Application Approved for Self-Employed")

    plt.show()


#function to plot top 3 months of transaction
def plot_top_3_mon(spark,USER,PASSWORD):
    """plot top 3 months of transaction"""
    cc_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CREDIT_CARD")
    dates_df=cc_df.loc[:,['TIMEID','TRANSACTION_ID']]
    dates_df['TIMEID']=pd.to_datetime(dates_df['TIMEID'],format='%Y%m%d').dt.month
    top_3=dates_df['TIMEID'].value_counts().sort_values().tail(3)
    top_3.plot(kind='barh',figsize=(10,5),xlim=(3900,3970),title='Top 3 Months of Transaction')
    for index,value in enumerate(top_3):
        plt.text(value+.20,index-0.1,str(value))
    plt.ylabel("Months")
    plt.xlabel("Number of transactions ")
    plt.show()


#plot which branch processed the highest total dollar value of healthcare transactions
def plot_healthcare(spark,USER,PASSWORD):
    """plot which branch processed highest total dollar value"""
    cc_df=load_data_df(spark,USER,PASSWORD,"CDW_SAPP_CREDIT_CARD")
    health_branch=cc_df[cc_df['TRANSACTION_TYPE']=='Healthcare'] \
             .groupby('BRANCH_CODE',as_index=False)['TRANSACTION_VALUE'] \
             .sum()\
             .sort_values(by='TRANSACTION_VALUE',ascending=False)
    br_col=colors(len(health_branch))
    health_branch.plot(kind='scatter',x='BRANCH_CODE',y='TRANSACTION_VALUE',figsize=(40,40)\
                   ,color=br_col,xlim=(0,200))
    plt.xlabel("Branch NO")
    plt.title("Total $value of Healthcare Transactions per Branch")
    br_hl_series=health_branch.set_index('BRANCH_CODE').squeeze()
    for index,value in br_hl_series.items():
        if value==br_hl_series.max(axis=0):
            plt.text(index-5,value-120,'Highest Total',bbox=dict(facecolor='yellow',alpha=0.5))
        plt.text(index+1,value-20,'#'+str(index))
    plt.show()

#calulate autopct/percentile
def func(pct, allvals):
    """autopct/percentile"""
    absolute = int(np.round(pct/100.*np.sum(allvals)))
    return f"{pct:.2f}%\n({absolute:d})"


#function to plot the percentage of rejection for married male applicants
def plot_m_m(spark,USER,PASSWORD):
    """plot percentile rejection for married men"""
    loan_df=load_data_df(spark,USER,PASSWORD,"cdw_sapp_loan_application")
    df_mm=loan_df[['Application_Status','Married','Gender']].value_counts()
    values=df_mm.values.tolist()
    status=['Approved Married Male','Not Approved Married Male','Approved Not Married Male',\
            'Not Approved Not Married Male','Approved Not Married Female','Not Approved Not Married Female',\
            'Approved Married Female','Not Approved Married Female']
    fig, ax = plt.subplots(figsize=(15, 8), subplot_kw=dict(aspect="equal"))
    def func(pct, allvals):
        absolute = int(np.round(pct/100.*np.sum(allvals)))
        return f"{pct:.2f}%\n({absolute:d})"
    wedges, texts, autotexts = ax.pie(values, autopct=lambda pct: func(pct, values),
                                    textprops=dict(color="w"),explode=(0,0.05,0,0,0,0,0,0))
    ax.legend(wedges, status, \
            title="Application Status", \
            loc="upper right", \
            bbox_to_anchor=(1, 0, 0.5, 1))

    plt.setp(autotexts, size=8, weight="bold")

    ax.set_title("Percentage of Rejection for Married Male Applicamts")

    plt.show()