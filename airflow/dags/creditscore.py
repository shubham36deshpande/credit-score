#!/usr/bin/env python
# coding: utf-8

# In[1]:
##################################
# process_snf_data:
# This function is for processing the data from tables:
# 1) psi_invoice, 2) chargebacks, 3) contract_status, 4) credit_memo 5) crm_classes
# 6) crm_customers 7) open_payables 8) open_payables 9) other_open_payables 10) unapplied_balance
# and generates tables:
# 1) agg_customer_score 2) agg_supplier_score 3) audit_agg_customer_score 4) audit_agg_supplier_score
# 5) audit_agg_customer_supplier_score 6) audit_ndg_score 7) Customer_suppier_score 8) ndg_score
# [it takes around 6.00-7.00 hrs to process]
##################################
import mysql.connector
import pandas as pd
import numpy as np
import math
import datetime
from datetime import datetime, timedelta, date
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings('ignore')
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, VARCHAR, TIMESTAMP, text
import os
import random

# import boto3
# import json

# client = boto3.client('secretsmanager', region_name='us-east-1')
# response = client.get_secret_value(SecretId='stg/snowflake/secrets')

# secret_string = response['SecretString']
# secret_dict = json.loads(secret_string)
# account = secret_dict['account']
# user = secret_dict['user']
# password = secret_dict['password']
# db = secret_dict['db']
# role = secret_dict['role']
# schema = secret_dict['schema']
# warehouse = secret_dict['warehouse']

def process_snf_data():

    account = 'blb89802.us-east-1'
    user = 'RGUPTA'
    password = 'Rachana.growexx@112'
    db = 'CREDIT_SCORE_PHASE2'
    schema = 'psi'
    warehouse = 'credit_score_wh'
    role = 'ACCOUNTADMIN'

    cnx = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        database=db,
        schema=schema,
        ocsp_response_cache_filename="/tmp/ocsp_response_cache"
    )


    # my_date = date(2024, 2, 5)
    # PSI_RAW_DATA.psi.
    date_query = (
        """ select processing_date from "Agg_customer_score" limit 1 """)

    try:
        process_date = pd.read_sql(date_query, cnx)

        print(process_date)
        df = process_date.loc[0]["PROCESSING_DATE"]
        print(type(df))
        my_date = df+timedelta(days=1)
        # my_date = df+timedelta(days=0)
        print(my_date)
    except:
        my_date = datetime.today().date() -timedelta(days=1) # added timedelta
        print(my_date)


    # ---------------------------------------------------------------------------------------------------------------

    def check_constant_values():
        # IDs
        id_c1 = 'c1'
        id_c2 = 'c2'
        id_c3 = 'c3'
        id_c4 = 'c4'
        id_c5 = 'c5'

        id_values = {id_c1:random.random(),
                     id_c2:random.random(),
                     id_c3:random.uniform(-1, 1),
                     id_c4:random.uniform(-1, 1),
                     id_c5:random.uniform(-1, 1)
                    }

        # Read the entire table
        constant_values = pd.read_sql('select * from credit_score_phase2.psi.constant_values;',cnx)
        constant_values.columns = constant_values.columns.str.lower()

        for id_ in [id_c1, id_c2, id_c3, id_c4, id_c5]:

            # Check if the record for id_ and today exists in the DataFrame
            data = constant_values[(constant_values['id'] == id_) & 
                                   (constant_values['processing_date'] == my_date)]


            if data.empty:
                # No entry for id_ today, check if there is any entry at all
                data_all = constant_values[constant_values['id'] == id_]

                if data_all.empty:
                    # No entry for id_ at all, create a new entry
                    constant_values = constant_values.append({'id': id_, 
                                                              'value': id_values[id_], 
                                                              'processing_date': my_date}, 
                                                             ignore_index=True)

                    print(f"Inserted new entry for {id_} into constant_values DataFrame")
                else:
                    # Entry for id_ exists, check processing_date
                    df_processing_date = data_all['processing_date'].max()

                    if df_processing_date < my_date:

                        constant_values.loc[constant_values['id'] == id_, ['value', 'processing_date']] = [id_values[id_], my_date]
                        print(f"Updated value for {id_} in constant_values DataFrame")
                    else:
                        # read processing_date
                        id_values[id_] = data_all['value'].values[0]
                        print(f"Using existing value for {id_}: {id_values[id_]}")
            else:
                # Entry for id_ today exists, read the value
                id_values[id_] = data['value'].values[0]
                print(f"Using existing value for {id_}: {id_values[id_]}")


        cs = cnx.cursor()
        file = 'constant_values'

        # '/home/ubuntu/Result/output.csv'
        constant_values.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

        cs.execute("Truncate Table CREDIT_SCORE_PHASE2.psi.constant_values;")

        cs.execute("""
            PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """ OVERWRITE=TRUE
        """)

        cs.execute("""
            COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
            FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
            FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
        """)

        cnx.commit()
        os.remove("/home/ubuntu/Result/output.csv")

    check_constant_values()
    constant_values = pd.read_sql('select * from CREDIT_SCORE_PHASE2.psi.constant_values;',cnx)
    constant_values.columns = constant_values.columns.str.lower()

    c1_value = constant_values[constant_values['id']=='c1'].values[0][1]
    c2_value = constant_values[constant_values['id']=='c2'].values[0][1]
    # ---------------------------------------------------------------------------------------------------------------




    invoice_sd = (my_date-timedelta(days=729)).strftime('%Y-%m-%d')  # 729
    invoice_ed = my_date.strftime('%Y-%m-%d')
    print(invoice_sd, invoice_ed)

    query8 = ("select * from PSI_RAW_DATA.psi.crm_classes")
    crm_classes = pd.read_sql(query8, cnx)
    crm_classes.columns = crm_classes.columns.str.lower()

    crm_classes['supplier_id'] = pd.to_numeric(crm_classes['supplier_id'], errors='coerce')




    query9 = ("select * from PSI_RAW_DATA.psi.crm_customers")
    crm_customers = pd.read_sql(query9, cnx)
    crm_customers.columns = crm_customers.columns.str.lower()

    crm_customers['customer_id'] = pd.to_numeric(crm_customers['customer_id'] , errors='coerce')
    crm_customers['ndg_id'] = pd.to_numeric(crm_customers['ndg_id'], errors='coerce')


    query = ("select * from PSI_RAW_DATA.psi.psi_invoice where invoice_date<=%s and invoice_date>=%s")
    PSI_invoice = pd.read_sql(query, cnx, params=(invoice_ed, invoice_sd))
    PSI_invoice.columns = PSI_invoice.columns.str.lower()

    PSI_invoice['customer_id'] = pd.to_numeric(PSI_invoice['customer_id'], errors='coerce')
    PSI_invoice['ndg_id'] = pd.to_numeric(PSI_invoice['ndg_id'], errors='coerce')
    PSI_invoice['supplier_id'] = pd.to_numeric(PSI_invoice['supplier_id'], errors='coerce')


    query1 = ("select * from PSI_RAW_DATA.psi.chargebacks where bill_date<=%s and bill_date>=%s")
    chargebacks = pd.read_sql(query1, cnx, params=(invoice_ed, invoice_sd))
    chargebacks.columns = chargebacks.columns.str.lower()

    chargebacks['customer_id'] = pd.to_numeric(chargebacks['customer_id'], errors='coerce')
    chargebacks['ndg_id'] = pd.to_numeric(chargebacks['ndg_id'], errors='coerce')
    chargebacks['supplier_id'] = pd.to_numeric(chargebacks['supplier_id'], errors='coerce')

    # Supplier

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    query2 = ("select * from PSI_RAW_DATA.psi.contract_status")
    contract_status = pd.read_sql(query2, cnx)


    contract_status.columns = contract_status.columns.str.lower()
    contract_status.rename(columns={'name': 'supplier', 'id':'supplier_id'}, inplace=True)

    contract_status = contract_status[['supplier_id','supplier', 'status_short', 'display_name']]

    contract_status['supplier_id'] = pd.to_numeric(contract_status['supplier_id'], errors='coerce')

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

    query3 = (
        "select * from PSI_RAW_DATA.psi.open_payable where invoice_date<=%s and invoice_date>=%s")
    open_payable = pd.read_sql(query3, cnx, params=(invoice_ed, invoice_sd))
    open_payable.columns = open_payable.columns.str.lower()

    open_payable['invoice_date'] = pd.to_datetime(open_payable['invoice_date'], errors='coerce').dt.date
    open_payable.rename(columns={'name': 'supplier'}, inplace=True)
    open_payable = open_payable[['id', 'client_id', 'supplier', 'status_short', 'display_name', 'open_payable']]

    open_payable['id'] = pd.to_numeric(open_payable['id'], errors='coerce')


    # @@@@@@@@@@@@@@@@@@@@@@@
    # open_payable = open_payable.groupby(['supplier'], as_index=False).agg({
    # 'status_short': 'first',
    # 'display_name': 'first',
    # 'open_payable': 'sum'})

    open_payable = open_payable.groupby(['id'], as_index=False).agg({
        'status_short': 'first',
        'display_name': 'first',
        'open_payable': 'sum'})


    # @@@@@@@@@@@@@@@@@@@@@@@

    query4 = (
        "select * from PSI_RAW_DATA.psi.unapplied_balance where invoice_date<=%s and invoice_date>=%s")
    unapplied_balance = pd.read_sql(
        query4, cnx, params=(invoice_ed, invoice_sd))
    unapplied_balance.columns = unapplied_balance.columns.str.lower()

    unapplied_balance['invoice_date'] = pd.to_datetime(unapplied_balance['invoice_date'], errors='coerce').dt.date
    unapplied_balance = unapplied_balance[
        ['id', 'client_id', 'supplier', 'status_short', 'display_name', 'unapplied_amount']]

    unapplied_balance['id'] = pd.to_numeric(unapplied_balance['id'], errors='coerce')

    # @@@@@@@@@@@@@@@@@@@@@@@
    # unapplied_balance = unapplied_balance.groupby(['supplier'], as_index=False).agg({
    #     'status_short': 'first',
    #     'display_name': 'first',
    #     'unapplied_amount': 'sum'})

    unapplied_balance = unapplied_balance.groupby(['id'], as_index=False).agg({
        'status_short': 'first',
        'display_name': 'first',
        'unapplied_amount': 'sum'})

    # @@@@@@@@@@@@@@@@@@@@@@@

    query5 = (
        "select * from PSI_RAW_DATA.psi.credit_memo where creditmemo_date<=%s and creditmemo_date>=%s")
    credit_memo = pd.read_sql(query5, cnx, params=(invoice_ed, invoice_sd))
    credit_memo.columns = credit_memo.columns.str.lower()

    credit_memo['creditmemo_date'] = pd.to_datetime(credit_memo['creditmemo_date'], errors='coerce').dt.date
    credit_memo = credit_memo[['supplier', 'client_id', 'status_short', 'display_name', 'amount']]

    # @@@@@@@@@@@@@@@@@@@@@@@
    # credit_memo = credit_memo.groupby(['supplier'], as_index=False).agg({
    #     'status_short': 'first',
    #     'display_name': 'first',
    #     'amount': 'sum'})


    # @@@@@@@@@@@@@@@@@@@@@@@@@@
    # Addding supplier id to credit_memo for groupby operation

    merged_df_ = pd.merge(credit_memo, crm_classes[['supplier_id','supplier']], on='supplier', how='left')

    credit_memo['supplier_id'] = merged_df_['supplier_id']

    credit_memo['supplier_id'] = pd.to_numeric(credit_memo['supplier_id'], errors='coerce')

    # -------------------------------------------------------------------------------------


    credit_memo = credit_memo.groupby(['supplier_id'], as_index=False).agg({
        'status_short': 'first',
        'display_name': 'first',
        'amount': 'sum'})
    # @@@@@@@@@@@@@@@@@@@@@@@

    credit_memo.rename(columns={'amount': 'open_credit_memo'}, inplace=True)


    query6 = (
        "select * from PSI_RAW_DATA.psi.other_open_payables where bill_date<=%s and bill_date>=%s")
    other_open_payables = pd.read_sql(
        query6, cnx, params=(invoice_ed, invoice_sd))
    other_open_payables.columns = other_open_payables.columns.str.lower()

    other_open_payables = other_open_payables[['client_id','supplier', 'display_name', 'status_short', 'other_open_payables']] 

    # @@@@@@@@@@@@@@@@@@@@@@@
    # other_open_payables = other_open_payables.groupby(['supplier', 'display_name', 'status_short'],
    #                                                   as_index=False).sum()

    other_open_payables['client_id'] = pd.to_numeric(other_open_payables['client_id'], errors='coerce')

    other_open_payables = other_open_payables.groupby(['client_id'],
                                                      as_index=False).sum()
    # @@@@@@@@@@@@@@@@@@@@@@@

    query7 = ("select * from PSI_RAW_DATA.psi.cash_balance")
    cash_balance = pd.read_sql(query7, cnx)
    cash_balance.columns = cash_balance.columns.str.lower()

    cash_balance = cash_balance[['id','supplier', 'display_name', 'status_short', 'cash_balance']]

    cash_balance['id'] =  pd.to_numeric(cash_balance['id'], errors='coerce')

    cash_balance = cash_balance[cash_balance['cash_balance'] != 0]

    crm_ndg = pd.read_sql("select * from PSI_RAW_DATA.psi.crm_ndg", cnx)
    crm_ndg.columns = crm_ndg.columns.str.lower()

    crm_ndg['id'] =  pd.to_numeric(crm_ndg['id'], errors='coerce')


    # # Customer Credit Score

    # # Invoice Data Preprocessing

    PSI_invoice['ndg_name'] = PSI_invoice['ndg_name'].fillna('None')

    # converted txn_amount=0 for Early payment advancement invoices whose transaction is not done
    mask = PSI_invoice['memo'].str.contains(
        r'Early payment advancement', na=False, case=False)
    invoice_num_counts = PSI_invoice['invoice_num'].value_counts()
    PSI_invoice.loc[mask & PSI_invoice['invoice_num'].isin(
        invoice_num_counts[invoice_num_counts == 1].index), 'txn_amount'] = 0

    # filter the rows where txn_type is memo and txn_amount is equal to invoice_amount
    creditmemo_mask = (PSI_invoice['txn_type'] == 'CreditMemo') & (
        abs(PSI_invoice['txn_amount']) == PSI_invoice['invoice_amount'])
    PSI_invoice = PSI_invoice[creditmemo_mask == False]
    PSI_invoice['invoice_date'] = pd.to_datetime(
        PSI_invoice['invoice_date'], errors='coerce').dt.date
    PSI_invoice['txn_date'] = pd.to_datetime(
        PSI_invoice['txn_date'], errors='coerce').dt.date
    PSI_invoice['duedate'] = pd.to_datetime(
        PSI_invoice['duedate'], errors='coerce').dt.date

    chargebacks = chargebacks[['ndg_id', 'ndg_name', 'customer_id', 'customer_name', 'customer_type_id', 'customer_type', 'supplier_id', 'client_name', 'bill_status', 'total_amount',
                               'open_balance', 'bill_date', 'cb_duedate', 'paid_date', 'paid_amount']]

    chargebacks['bill_date'] = pd.to_datetime(
        chargebacks['bill_date'], errors='coerce').dt.date
    chargebacks['cb_duedate'] = pd.to_datetime(
        chargebacks['cb_duedate'], errors='coerce').dt.date
    chargebacks['paid_date'] = pd.to_datetime(
        chargebacks['paid_date'], errors='coerce').dt.date

    # # Concat Invoice and Chargeback records

    # In[11]:

    # stack both tables on customer and supplier names
    Data_processed = pd.concat([PSI_invoice, chargebacks], axis=0, sort=True)

    # Correction: 1. Data[duedate] = invoice date+terms

    # In[12]:

    # fill missing values with zero
    Data_processed[['balance', 'invoice_amount', 'open_balance', 'paid_amount', 'total_amount', 'txn_amount']] = Data_processed[[
        'balance', 'invoice_amount', 'open_balance', 'paid_amount', 'total_amount', 'txn_amount']].fillna(0)
    Data_processed['duedate'].fillna(
        Data_processed['invoice_date']+timedelta(days=30), inplace=True)
    Data_processed['ispaid'] = Data_processed['ispaid'].fillna(2)
    Data_processed['ndg_name'] = Data_processed['ndg_name'].replace(
        np.nan, 'None')
    Data_processed['ndg_id'] = Data_processed['ndg_id'].replace(np.nan, -1)



    # # Invoice Parameter

    # In[13]:

    map_df = pd.DataFrame()
    Customer = pd.DataFrame()
    Customer_normalisation = pd.DataFrame()
    weight_list = []

    # @@@@@@@@@@@@@@@@@@@@@@@
    # customer_list = Data_processed['customer_name'].unique().tolist()

    customer_list = Data_processed['customer_id'].unique().tolist()

    # 1044,
    # customer_list =[ 988,1028 ,10306 ,10427 ,1044,1970,26086 ,2238, 11466, 4096,33844]
    # @@@@@@@@@@@@@@@@@@@@@@@

    historical_cust_supp_map = {'customer_id':[],
                            'customer':[],
                            'supplier_id':[], 
                            'supplier':[]}


    for cust_count,i in enumerate(customer_list):

        empty_data_invoice_temp_flag = 0

        Aggregated_invoice_table = pd.DataFrame()

        # @@@@@@@@@@@@@@@@@@@@@@@
        # supp_values = Data_processed[Data_processed['customer_name']
        #                              == i]['client_name'].unique().tolist()

        supp_values = Data_processed[Data_processed['customer_id']
                             == i]['supplier_id'].unique().tolist()

        
        # Remove NaN values 
        supp_values = [x for x in supp_values if not np.isnan(x)]


        # @@@@@@@@@@@@@@@@@@@@@@@


        supplier_list = [x for x in supp_values if x is not None]


        # @@@@@@@@@@@@@@@@@@@@@@@
        # cust_id_ = Data_processed[Data_processed['customer_name']== i]['customer_id'].values[0]

        cust_id_ = Data_processed[Data_processed['customer_id']== i]['customer_id'].values[0]
        # @@@@@@@@@@@@@@@@@@@@@@@

        count = 1
        print(cust_count,i, Data_processed[Data_processed['customer_id']== i]['customer_name'].values[0])
        for supp_count,j in enumerate(supplier_list):
            print('\t',supp_count, j,Data_processed[Data_processed['supplier_id'] == j]['client_name'].values[0])


            # @@@@@@@@@@@@@@@@@@@@@@@    
            # supp_id_ = Data_processed[Data_processed['client_name'] == j]['supplier_id'].values[0]

            supp_id_ = Data_processed[Data_processed['supplier_id'] == j]['supplier_id'].values[0]


            # @@@@@@@@@@@@@@@@@@@@@@@


            historical_cust_supp_map['customer_id'].append(cust_id_)
            historical_cust_supp_map['customer'].append(i)
            historical_cust_supp_map['supplier_id'].append(supp_id_)
            historical_cust_supp_map['supplier'].append(j)


            Aggregated_table_temp = pd.DataFrame()



            # @@@@@@@@@@@@@@@@@@@@@@@
            # data_invoice_temp = Data_processed[(Data_processed['customer_name'] == i) & (
            #     Data_processed['client_name'] == j) & (Data_processed['ispaid'] != 2)]

            data_invoice_temp = Data_processed[(Data_processed['customer_id'] == i) & (
                Data_processed['supplier_id'] == j) & (Data_processed['ispaid'] != 2)]

            # @@@@@@@@@@@@@@@@@@@@@@@

    # --------------------------------------------------------------------------------------------------        
            # this is to check that the data is comming from the chargeback table or not
            # and due to this data_invoice_temp will be empty which in result gives 255 score as final result
            if len(supplier_list) == 1 and len(data_invoice_temp) == 0:
                print("\t\t*****")
                empty_data_invoice_temp_flag = 1
    # --------------------------------------------------------------------------------------------------   

            # @@@@@@@@@@@@@@@@@@@@@@@
            # customer_name = i
            # Supplier_name = j


            customer_name = Data_processed[Data_processed['customer_id']== i]['customer_name'].values[0]
            Supplier_name = Data_processed[Data_processed['supplier_id'] == j]['client_name'].values[0]
            # @@@@@@@@@@@@@@@@@@@@@@@

            # ndg_id = Data_processed[(
            #     Data_processed['customer_name'] == i)]['ndg_id'].iloc[0]
            # ndg_name = Data_processed[(
            #     Data_processed['customer_name'] == i)]['ndg_name'].iloc[0]


             # @@@@@@@@@@@@@@@@@@@@@@@
            # Changes wrt Feedback 8
            # try:
            #     ndg_id = Data_processed[(
            #         Data_processed['customer_name'] == i) & (Data_processed['ndg_id'] != -1)]['ndg_id'].iloc[0]
            #     ndg_name = Data_processed[(
            #         Data_processed['customer_name'] == i) & (Data_processed['ndg_id'] != -1)]['ndg_name'].iloc[0]

            # except:
            #     ndg_id = Data_processed[(
            #             Data_processed['customer_name'] == i)]['ndg_id'].iloc[0]
            #     ndg_name = Data_processed[(
            #             Data_processed['customer_name'] == i)]['ndg_name'].iloc[0]
             # @@@@@@@@@@@@@@@@@@@@@@@

            try:
                ndg_id = Data_processed[(
                    Data_processed['customer_id'] == i) & (Data_processed['ndg_id'] != -1)]['ndg_id'].iloc[0]
                ndg_name = Data_processed[(
                    Data_processed['customer_id'] == i) & (Data_processed['ndg_id'] != -1)]['ndg_name'].iloc[0]

            except:
                ndg_id = Data_processed[(
                        Data_processed['customer_id'] == i)]['ndg_id'].iloc[0]
                ndg_name = Data_processed[(
                        Data_processed['customer_id'] == i)]['ndg_name'].iloc[0]


            # @@@@@@@@@@@@@@@@@@@@@@@
            # customer_type = Data_processed[(
            #     Data_processed['customer_name'] == i)]['customer_type'].iloc[0]
            # customer_type_id = Data_processed[(
            #     Data_processed['customer_name'] == i)]['customer_type_id'].iloc[0]
            # customer_id = Data_processed[(
            #     Data_processed['customer_name'] == i)]['customer_id'].iloc[0]
            # supplier_id = Data_processed[(
            #     Data_processed['client_name'] == j)]['supplier_id'].iloc[0]



            customer_type = Data_processed[(
                Data_processed['customer_id'] == i)]['customer_type'].iloc[0]
            customer_type_id = Data_processed[(
                Data_processed['customer_id'] == i)]['customer_type_id'].iloc[0]
            customer_id = Data_processed[(
                Data_processed['customer_id'] == i)]['customer_id'].iloc[0]
            supplier_id = Data_processed[(
                Data_processed['supplier_id'] == j)]['supplier_id'].iloc[0]

            # @@@@@@@@@@@@@@@@@@@@@@@

            if len(data_invoice_temp) != 0:

                # Open AR
                unpaid_invoices = data_invoice_temp[data_invoice_temp['ispaid'] == 0]
                # Closed AR
                paid_invoices = data_invoice_temp[data_invoice_temp['ispaid'] == 1]

                if len(data_invoice_temp['invoice_date']) != 0:
                    Payment_predictor1 = (pd.to_datetime(
                        data_invoice_temp['duedate'])-pd.to_datetime(data_invoice_temp['invoice_date'])).max().days
                else:
                    Payment_predictor1 = 0

                open_Invoices = (paid_invoices.groupby('invoice_num')['invoice_amount'].mean(
                ).sum())+(unpaid_invoices.groupby('invoice_num')['invoice_amount'].mean().sum())

                open_AR_txn_vector = unpaid_invoices[unpaid_invoices['txn_date'] <= my_date].groupby(
                    'invoice_num')['txn_amount'].sum()

                if len(open_AR_txn_vector) != 0:
                    open_AR_txn = abs(open_AR_txn_vector.sum())
                else:
                    open_AR_txn = 0

                open_AR_txn_vector1_temp = unpaid_invoices[unpaid_invoices['duedate'] < my_date]
                open_AR_txn_vector1 = open_AR_txn_vector1_temp[open_AR_txn_vector1_temp['txn_date'] <= my_date].groupby('invoice_num')[
                    'txn_amount'].sum()
                if len(open_AR_txn_vector1) != 0:
                    open_AR_txn1 = abs(open_AR_txn_vector1.sum())
                else:
                    open_AR_txn1 = 0

                total_AR = (unpaid_invoices.groupby('invoice_num')[
                            'invoice_amount'].mean().sum()) - open_AR_txn
                open_AR = (open_AR_txn_vector1_temp.groupby('invoice_num')[
                           'invoice_amount'].mean().sum()) - open_AR_txn1

                paid_perc = (((paid_invoices.groupby('invoice_num')[
                             'invoice_amount'].mean().sum())+open_AR_txn)/open_Invoices)*100
                unpaid_perc = (total_AR/open_Invoices)*100

                # --------- Paid invoices further divided into early and late payment-----------------

                early_payment_paid_invoices1 = paid_invoices[paid_invoices['txn_date']
                                                             <= paid_invoices['duedate']]
                late_payment_paid_invoices1 = paid_invoices[paid_invoices['txn_date']
                                                            > paid_invoices['duedate']]

                early_payment_paid_invoices2 = unpaid_invoices[(
                    unpaid_invoices['txn_date'] <= unpaid_invoices['duedate']) & (unpaid_invoices['txn_date'] <= my_date)]
                late_payment_paid_invoices2 = unpaid_invoices[(
                    unpaid_invoices['txn_date'] > unpaid_invoices['duedate']) & (unpaid_invoices['txn_date'] <= my_date)]

                early_payment_paid_invoices = pd.concat(
                    [early_payment_paid_invoices1, early_payment_paid_invoices2])
                late_payment_paid_invoices = pd.concat(
                    [late_payment_paid_invoices1, late_payment_paid_invoices2])

                if len(early_payment_paid_invoices1) != 0:
                    early_payment_perc = (
                        abs(early_payment_paid_invoices['txn_amount'].sum())/open_Invoices)*100
                    early_payment_amount = abs(
                        early_payment_paid_invoices['txn_amount'].sum())
                    early_payment_days = (pd.to_datetime(early_payment_paid_invoices1['duedate'])-pd.to_datetime(
                        early_payment_paid_invoices1['txn_date'])).mean().days
                else:
                    early_payment_perc = 0
                    early_payment_amount = 0
                    early_payment_days = 0

                unpaid_invoices_duedate_before_mydate = unpaid_invoices[
                    unpaid_invoices['duedate'] <= my_date]
                unpaid_invoices_duedate_after_mydate = unpaid_invoices[
                    unpaid_invoices['duedate'] >= my_date]

                invoice_freq = len(data_invoice_temp.groupby('invoice_num'))

                scaler = MinMaxScaler(feature_range=(-1, 1))
                if len(paid_invoices) != 0:
                    Payment_Timeliness_Ratio = (
                        len(early_payment_paid_invoices1)/len(paid_invoices))*100

                    Payment_Cycle_p1 = (pd.to_datetime(paid_invoices[pd.isna(paid_invoices['txn_date']) == False]['txn_date'])-pd.to_datetime(
                        paid_invoices[pd.isna(paid_invoices['txn_date']) == False]['invoice_date'])).reset_index(drop=True).dt.days

                    Payment_Cycle_p2 = (paid_invoices[pd.isna(
                        paid_invoices['txn_date']) == False]['txn_amount']).reset_index(drop=True)

                    if len(Payment_Cycle_p1) != 0:
                        Payment_Cycle = round(
                            (Payment_Cycle_p1*Payment_Cycle_p2).sum()/Payment_Cycle_p2.sum())
                    else:
                        Payment_Cycle = 0

                    Days_Beyond_Terms_p1 = (pd.to_datetime(paid_invoices[pd.isna(paid_invoices['txn_date']) == False]['txn_date'])-pd.to_datetime(
                        paid_invoices[pd.isna(paid_invoices['txn_date']) == False]['duedate'])).reset_index(drop=True).dt.days

                    if len(Days_Beyond_Terms_p1) != 0:
                        Days_Beyond_Terms = round(
                            (Days_Beyond_Terms_p1*Payment_Cycle_p2).sum()/Payment_Cycle_p2.sum())
                    else:
                        Days_Beyond_Terms = 0
                else:
                    Payment_Timeliness_Ratio = 0
                    Days_Beyond_Terms = 0
                    Payment_Cycle = 0

                if len(unpaid_invoices) != 0:
                    Aging_Balance_p1 = (pd.to_datetime(
                        my_date)-pd.to_datetime(unpaid_invoices['duedate'])).reset_index(drop=True).dt.days
                    Aging_Balance_p2 = (unpaid_invoices.groupby('invoice_num')[
                                        'invoice_amount'].mean()).reset_index(drop=True)
                    Aging_Balance = round(
                        (Aging_Balance_p1*Aging_Balance_p2).sum()/Aging_Balance_p2.sum())
                    if Aging_Balance <= 10:
                        Aging_Balance = 0

                else:
                    Aging_Balance = 0

                if len(unpaid_invoices_duedate_after_mydate) != 0:
                    outstanding_balance_vector = unpaid_invoices_duedate_after_mydate[unpaid_invoices_duedate_after_mydate['txn_date'] <= my_date].groupby('invoice_num')[
                        'txn_amount'].sum()
                    if len(outstanding_balance_vector) != 0:
                        outstanding_balance_txn = abs(
                            outstanding_balance_vector.sum())
                    else:
                        outstanding_balance_txn = 0
                    outstanding_balance = ((unpaid_invoices_duedate_after_mydate.groupby(
                        'invoice_num')['invoice_amount'].mean().sum())-(outstanding_balance_txn))
                else:
                    outstanding_balance = 0
                    
                    
                # @@@@@@@@@@@@@@@@@@@
    #             num_supplier1 = data_invoice_temp['client_name'].nunique()
    #             num_supplier_w_open_invoice = unpaid_invoices['client_name'].nunique(
    #             )
                num_supplier1 = data_invoice_temp['supplier_id'].nunique()
                num_supplier_w_open_invoice = unpaid_invoices['supplier_id'].nunique()

                
                # @@@@@@@@@@@@@@@@@@@@
                # ------------delinquent_payment_within_10_days of due date-------------------

                temp_delinquent_payment_before_10_days = late_payment_paid_invoices[(
                    (late_payment_paid_invoices['txn_date'])-(late_payment_paid_invoices['duedate'])) <= timedelta(days=10)]
                temp_delinquent_payment_before_10_days['overdue_days'] = pd.to_datetime(
                    temp_delinquent_payment_before_10_days['txn_date']) - pd.to_datetime(temp_delinquent_payment_before_10_days['duedate'])

                temp_delinquent_payment_before_10_days1 = late_payment_paid_invoices1[(
                    (late_payment_paid_invoices1['txn_date'])-(late_payment_paid_invoices1['duedate'])) <= timedelta(days=10)]

                if len(temp_delinquent_payment_before_10_days1) != 0:
                    latepayment_ratio_before_10_day = (
                        (len(temp_delinquent_payment_before_10_days1))/(len(paid_invoices)))*100
                else:
                    latepayment_ratio_before_10_day = 0

                if len(temp_delinquent_payment_before_10_days) != 0:
                    payment1_percent = (
                        (abs(temp_delinquent_payment_before_10_days['txn_amount'].sum()))/open_Invoices)*100
                    delinquent_payment_before_10_days_percent = abs(
                        temp_delinquent_payment_before_10_days['txn_amount'].sum())
                    delinquent_payment_before_10_days_overdueDays = temp_delinquent_payment_before_10_days['overdue_days'].mean(
                    ).days
                else:
                    payment1_percent = 0
                    delinquent_payment_before_10_days_percent = 0
                    delinquent_payment_before_10_days_overdueDays = 0

                # ------------delinquent_payment_outside_10_days of due date-------------------

                temp_delinquent_payment_after_10_days = late_payment_paid_invoices[(
                    (late_payment_paid_invoices['txn_date'])-(late_payment_paid_invoices['duedate'])) > timedelta(days=10)]
                temp_delinquent_payment_after_10_days['overdue_days'] = pd.to_datetime(
                    temp_delinquent_payment_after_10_days['txn_date']) - pd.to_datetime(temp_delinquent_payment_after_10_days['duedate'])

                temp_delinquent_payment_after_10_days1 = late_payment_paid_invoices1[(
                    (late_payment_paid_invoices1['txn_date'])-(late_payment_paid_invoices1['duedate'])) > timedelta(days=10)]

                if len(temp_delinquent_payment_after_10_days1) != 0:
                    latepayment_ratio_after_10_day = (
                        (len(temp_delinquent_payment_after_10_days1))/(len(paid_invoices)))*100
                else:
                    latepayment_ratio_after_10_day = 0

                if len(temp_delinquent_payment_after_10_days) != 0:
                    payment2_percent = (
                        (abs(temp_delinquent_payment_after_10_days['txn_amount'].sum()))/open_Invoices)*100
                    delinquent_payment_after_10_days_percent = abs(
                        temp_delinquent_payment_after_10_days['txn_amount'].sum())
                    delinquent_payment_after_10_days_overdueDays = temp_delinquent_payment_after_10_days['overdue_days'].mean(
                    ).days
                    Payment_predictor = Payment_predictor1 + \
                        delinquent_payment_after_10_days_overdueDays
                else:
                    payment2_percent = 0
                    delinquent_payment_after_10_days_percent = 0
                    delinquent_payment_after_10_days_overdueDays = 0
                    Payment_predictor = Payment_predictor1 + \
                        delinquent_payment_after_10_days_overdueDays
                payment2_percent_wo_cb = 0.01

            else:
                Payment_predictor = 0
                early_payment_perc = 0
                early_payment_amount = 0
                early_payment_days = 0
                invoice_freq = 0
                open_Invoices = 0
                open_AR = 0
                Aging_Balance = 0
                paid_perc = 0
                unpaid_perc = 0
                outstanding_balance = 0
                Payment_Timeliness_Ratio = 0
                latepayment_ratio_before_10_day = 0
                latepayment_ratio_after_10_day = 0
                Days_Beyond_Terms = 0
                Payment_Cycle = 0
                delinquent_payment_before_10_days_percent = 0
                payment1_percent = 0
                delinquent_payment_before_10_days_overdueDays = 0
                delinquent_payment_after_10_days_percent = 0
                payment2_percent = 0
                delinquent_payment_after_10_days_overdueDays = 0
                num_supplier1 = 0
                payment1_percent = 0
                payment2_percent = 0
                Payment_predictor = 0
                num_supplier_w_open_invoice = 0
                payment2_percent_wo_cb = 0
                total_AR = 0

                
            # @@@@@@@@@@@@@@@@@@@@@@@@@@@
    #         chargebacks_temp = chargebacks[(chargebacks['customer_name'] == i) & (
    #             chargebacks['client_name'] == j)]

            chargebacks_temp = chargebacks[(chargebacks['customer_id'] == i) & (
                chargebacks['supplier_id'] == j)]
            
            # @@@@@@@@@@@@@@@@@@@@@@@@@@@
            
            
            if len(chargebacks_temp) != 0:
                
                # @@@@@@@@@@@@@@@@@@@@@@@@@@@
    #             num_supplier2 = chargebacks_temp['client_name'].nunique()
                
                num_supplier2 = chargebacks_temp['supplier_id'].nunique()
                
                # @@@@@@@@@@@@@@@@@@@@@@@@@@@
                
                chargeback_freq = len(chargebacks_temp)
                chargeback_vol_amt = abs(
                    chargebacks_temp['total_amount']).sum()
                chargeback_vol_paid = (
                    abs(chargebacks_temp['total_amount'])-chargebacks_temp['open_balance']).sum()

                overdue_chargebacks = abs(chargebacks_temp[(chargebacks_temp['open_balance'] != 0) & (
                    (chargebacks_temp['cb_duedate']) < my_date)]['paid_amount']).sum()

                early_chargeback = abs(chargebacks_temp[(chargebacks_temp['open_balance'] == 0) & (
                    (chargebacks_temp['paid_date']) <= (chargebacks_temp['cb_duedate']))]['paid_amount']).sum()
                early_chargeback_leng = len(chargebacks_temp[(chargebacks_temp['open_balance'] == 0) & (
                    (chargebacks_temp['paid_date']) <= (chargebacks_temp['cb_duedate']))]['paid_amount'])

                if len(chargebacks_temp[(chargebacks_temp['open_balance'] != 0)]) == 0:
                    open_chargeback_freq = 0
                    open_chargeback_vol_amt = 0
                else:
                    open_chargeback_freq = len(
                        chargebacks_temp[(chargebacks_temp['open_balance'] != 0)])
                    open_chargeback_vol_amt = abs(
                        chargebacks_temp[(chargebacks_temp['open_balance'] != 0)]['total_amount']).sum()

                if len(chargebacks_temp[(chargebacks_temp['open_balance'] == 0)]) == 0:
                    Timeliness_of_chargeback = 0
                else:
                    Timeliness_of_chargeback = (
                        early_chargeback_leng/len(chargebacks_temp[(chargebacks_temp['open_balance'] == 0)]))*100
                
                # @@@@@@@@@@@@@@@@@@@@@@@@
    #             supplier_wo_overdue_cb = Data_processed[(Data_processed['paid_date'] <= Data_processed['cb_duedate']) & (
    #                 Data_processed['ispaid'] == 2)]['client_name'].nunique()
                
    #             supplier_w_overdue_cb = Data_processed[(Data_processed['paid_date'] > Data_processed['cb_duedate']) & (
    #                 Data_processed['ispaid'] == 2)]['client_name'].nunique()
                
        
                supplier_wo_overdue_cb = Data_processed[(Data_processed['paid_date'] <= Data_processed['cb_duedate']) & (
                    Data_processed['ispaid'] == 2)]['supplier_id'].nunique()
                
                supplier_w_overdue_cb = Data_processed[(Data_processed['paid_date'] > Data_processed['cb_duedate']) & (
                    Data_processed['ispaid'] == 2)]['supplier_id'].nunique()
                
                # @@@@@@@@@@@@@@@@@@@@@@@@@
                
                chargeback_vol_unpaid = abs(
                    chargebacks_temp['open_balance']).sum()
            else:
                chargeback_freq = 0
                chargeback_vol_paid = 0
                chargeback_vol_amt = 0
                chargeback_vol_unpaid = 0
                overdue_chargebacks = 0
                early_chargeback = 0
                Timeliness_of_chargeback = 0
                supplier_wo_overdue_cb = 0
                supplier_w_overdue_cb = 0
                num_supplier2 = 0
                open_chargeback_freq = 0
                open_chargeback_vol_amt = 0

            num_supplier = num_supplier1 + num_supplier2

            # if invoice_freq != 0:
            #     wo_open_cb = (
            #         (invoice_freq-(chargeback_freq-open_chargeback_freq))/invoice_freq)*100
            # else:
            #     wo_open_cb = 0

            # Changed the condition as per client's requirement.

            if invoice_freq != 0 and invoice_freq > chargeback_freq:
                wo_open_cb = ((invoice_freq-(chargeback_freq-open_chargeback_freq))/invoice_freq)*100
            else:
                wo_open_cb = 0

            Risk_balance = total_AR - chargeback_vol_unpaid

            output = {
                'ndg_id': [ndg_id],
                'ndg_name': [ndg_name],
                'customer_id': [customer_id],
                'supplier_id': [supplier_id],
                'Customer': [customer_name],
                'supplier': [Supplier_name],
                'Customer_type_id': [customer_type_id],
                'Customer type': [customer_type],
                'invoice_freq': [invoice_freq],
                'Open Invoices': [open_Invoices],
                'Paid Invoice (%)': [paid_perc],
                'Unpaid Invoice (%)': [unpaid_perc],
                'early_payment_perc': [early_payment_perc],
                'early_payment_amount': [early_payment_amount],
                'early_payment_days': [early_payment_days],
                'Open AR': [open_AR],
                'Aging Balance': [Aging_Balance],
                'Outstanding Balance': [outstanding_balance],
                'Payment Timeliness Ratio (%)': [Payment_Timeliness_Ratio],
                'Late Payment Ratio<10_days': [latepayment_ratio_before_10_day],
                'Late Payment Ratio>10_days': [latepayment_ratio_after_10_day],
                'DBT': [Days_Beyond_Terms],
                'Payment Cycle': [Payment_Cycle],
                'Delinquent Payment<10_days': [delinquent_payment_before_10_days_percent],
                'Delinquent Overdueday<10_days': [delinquent_payment_before_10_days_overdueDays],
                'Delinquent Payment>10_days': [delinquent_payment_after_10_days_percent],
                'Delinquent Overdueday>10_days': [delinquent_payment_after_10_days_overdueDays],
                'Payment_predictor': [Payment_predictor],
                '# Suppliers': [num_supplier],
                'chargeback_freq': [chargeback_freq],
                'chargeback_vol_amt': [chargeback_vol_amt],
                'chargeback_vol_paid': [chargeback_vol_paid],
                'chargeback_vol_unpaid': [chargeback_vol_unpaid],
                'supplier_wo_overdue_cb': [supplier_wo_overdue_cb],
                'supplier_w_overdue_cb': [supplier_w_overdue_cb],
                'overdue chargebacks': [overdue_chargebacks],
                'early_chargeback': [early_chargeback],
                'Timeliness of chargeback': [Timeliness_of_chargeback],
                'Risk balance': [Risk_balance],
                'w/o open cb': [wo_open_cb],
                'total_AR': [total_AR],
                'payment1_percent': [payment1_percent],
                'payment2_percent': [payment2_percent],
                'open_chargeback_freq': [open_chargeback_freq],
                'open_chargeback_vol_amt': [open_chargeback_vol_amt],
                'num_supplier_w_open_invoice': [num_supplier_w_open_invoice],
                'payment2_percent_wo_cb': [payment2_percent_wo_cb]
            }
            Aggregated_table_temp = pd.DataFrame(output)

            if count == 1:
                Aggregated_invoice_table = Aggregated_table_temp
            else:
                Aggregated_invoice_table = pd.concat(
                    [Aggregated_invoice_table, Aggregated_table_temp])
            count = count+1

        # correlation

        customer_metric = Aggregated_invoice_table.drop(columns=['Payment_predictor', 'ndg_id', 'Customer_type_id', 'customer_id', 'supplier_id', 'payment1_percent', 'payment2_percent', 'ndg_name', 'Customer', 'supplier', 'Customer type',
                                                                 'invoice_freq', '# Suppliers', 'supplier_wo_overdue_cb', 'chargeback_vol_paid', 'chargeback_vol_unpaid',
                                                                 'Timeliness of chargeback', 'w/o open cb', 'early_payment_perc', 'early_payment_days', 'total_AR', 'early_payment_amount', 'open_chargeback_vol_amt', 'open_chargeback_freq', 'supplier_w_overdue_cb', 'num_supplier_w_open_invoice', 'payment2_percent_wo_cb'])
        corr_df1 = customer_metric.drop(columns=['Risk balance'])
        corr_df2 = customer_metric['Risk balance']

        keys_to_change_p = ['Paid Invoice (%)', 'Open Invoices', 'Payment Timeliness Ratio (%)', 
                'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days']

        keys_to_change_n = ['Open AR', 'Aging Balance', 'Outstanding Balance',
                'Late Payment Ratio>10_days', 'DBT', 
                'Payment Cycle', 'Delinquent Payment>10_days', 
                'Delinquent Overdueday>10_days', 
                'chargeback_vol_amt', 'overdue chargebacks']

        if ((customer_metric['Risk balance'] <= 0).all()) | (len(supplier_list) == 1):

            # weights = {'Open Invoices': 0.3,
            #            'Paid Invoice (%)': 0.3,
            #            'Unpaid Invoice (%)': -0.31,
            #            'Open AR': -0.5,
            #            'Aging Balance': -0.05,
            #            'Outstanding Balance': 0,
            #            'Payment Timeliness Ratio (%)': 0.33,
            #            'Late Payment Ratio<10_days': 0.33,
            #            'Late Payment Ratio>10_days': -0.33,
            #            'DBT': -1,
            #            'Payment Cycle': -0.3,
            #            'Delinquent Payment<10_days': 0.14,
            #            'Delinquent Overdueday<10_days': -0.1,
            #            'Delinquent Payment>10_days': -0.2,
            #            'Delinquent Overdueday>10_days': -0.2,
            #            'chargeback_freq': -0.07,
            #            'chargeback_vol_amt': -0.07,
            #            'overdue chargebacks': -0.06,
            #            'early_chargeback': -0.07
            #            }

            # Changed Open Invoices to negative :: Reason :: Open Invoices is = Total AR/Open AR [MAIL] --
            # Changed Open Invoices to positive ++ :: Reason :: Open Invoice is actually total Invoice
            # Changed Unpaid Invoice to 0 :: Reason :: No mentioned in the list.
            # Changed Late Payment Ratio<10_days to 1 :: Reason :: ML decided whether it would be positive or negative on a per customer basis.
            # Changed chargeback_freq to 0 :: Reason :: No mentioned in the list.
            # Changed early_chargeback to 0 :: Reason :: No mentioned in the list.

            weights = {'Open Invoices': 0.3,
                       'Paid Invoice (%)': 0.3,
                       'Unpaid Invoice (%)': 0,
                       'Open AR': -0.5,
                       'Aging Balance': -0.05,
                       'Outstanding Balance': 0,
                       'Payment Timeliness Ratio (%)': 0.33,
                       'Late Payment Ratio<10_days': 1,
                       'Late Payment Ratio>10_days': -0.33,
                       'DBT': -1,
                       'Payment Cycle': -0.3,
                       'Delinquent Payment<10_days': 0.14,
                       'Delinquent Overdueday<10_days': 0.1,
                       'Delinquent Payment>10_days': -0.2,
                       'Delinquent Overdueday>10_days': -0.2,
                       'chargeback_freq': 0,
                       'chargeback_vol_amt': -0.07,
                       'overdue chargebacks': -0.06,
                       'early_chargeback': 0
                       }
        else:

            corr_matrix = corr_df1.corrwith(corr_df2)
            weights = {col: corr_matrix[col] for col in corr_matrix.index}

            for key, value in weights.items():
                if math.isnan(value):
                    weights[key] = 0
            # key to change
            # keys_to_change_p = [
            #     'Paid Invoice (%)', 'Open Invoices', 'Payment Timeliness Ratio (%)']


            for key in keys_to_change_p:

                if weights[key] < 0:
                    weights[key] *= -1

            # keys_to_change_n = ['Unpaid Invoice (%)', 'Aging Balance', 'Outstanding Balance', 'DBT', 'Open AR',
            #                     'chargeback_freq', 'chargeback_vol_amt', 'Late Payment Ratio<10_days', 'Late Payment Ratio>10_days', 'Payment Cycle']



            for key in keys_to_change_n:
                if weights[key] > 0:
                    weights[key] *= -1


            keys_with_no_impact = ['Unpaid Invoice (%)', 'chargeback_freq', 'early_chargeback']

            for key in keys_with_no_impact:
                weights[key] = 0

        weight_list.append(weights)

        Customer_norm = (corr_df1 - corr_df1.mean()) / (corr_df1.std())
        Customer_norm.fillna(0, inplace=True)

    # -----------------------------------------------------------------------------------------------------

        min_max_constant = 0.01
        for row in Customer_norm.columns:
            for idx in range(len(Customer_norm)):

        #         print(row)
        #         print(Customer_norm[row].values[idx])
        #         print(Aggregated_invoice_table[row].values[idx])
                if Aggregated_invoice_table[row].values[idx]== 0.0:
                    Customer_norm[row].values[idx] = 0

        Customer_norm_max_min = ((Customer_norm - Customer_norm.min()) + min_max_constant) / ((Customer_norm.max() - Customer_norm.min()) + min_max_constant)
        Customer_norm_max_min.fillna(0, inplace=True)

        Customer_norm = Customer_norm_max_min

        for row in Customer_norm.columns:
            for idx in range(len(Customer_norm)):

        #         print(row)
        #         print(Customer_norm[row].values[idx])
        #         print(Aggregated_invoice_table[row].values[idx])
                if Aggregated_invoice_table[row].values[idx]== 0.0:
                    Customer_norm[row].values[idx] = 0

    # -----------------------------------------------------------------------------------------------------

    # --------------------------------------------------------------------------------------------------
        if len(supplier_list) == 1:
    #         print("Old Customer_norm values :: ", Customer_norm.values)
            print("Calculating new Customer norm values ....")
            positive_impact_param_mean = corr_df1[keys_to_change_p].values[0].mean()

            positive_impact_param_std = corr_df1[keys_to_change_p].values[0].std()

    #         print("Mean of positive impact parameters : ", positive_impact_param_mean)
    #         print("standard deviation of positive impact parameters : ", positive_impact_param_std)

    #         print("Keys to positive ", keys_to_change_p)
            for key in keys_to_change_p:
    #             print(key)
                Customer_norm[key] = (corr_df1[key] - positive_impact_param_mean)/positive_impact_param_std

            negative_impact_param_mean = corr_df1[keys_to_change_n].values[0].mean()

            negative_impact_param_std = corr_df1[keys_to_change_n].values[0].std()

    #         print("Mean of negative impact parameters : ", negative_impact_param_mean)
    #         print("standard deviation of negative impact parameters : ", negative_impact_param_std)

    #         print("Keys to negative ", keys_to_change_n)

            for key in keys_to_change_n:
    #             print(key)
                Customer_norm[key] = (corr_df1[key] - negative_impact_param_mean)/negative_impact_param_std

    #         print("Updated Customer_norm values :: ", Customer_norm.values)

            for row in Customer_norm.columns:
                for idx in range(len(Customer_norm)):

            #         print(row)
            #         print(Customer_norm[row].values[idx])
            #         print(Aggregated_invoice_table[row].values[idx])
                    if Aggregated_invoice_table[row].values[idx]== 0.0:
                        Customer_norm[row].values[idx] = 0

            keys_to_change_n1 = ['Paid Invoice (%)', 'Payment Timeliness Ratio (%)', 
                         'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days','Open Invoices','Open AR',
                         'Aging Balance', 'Outstanding Balance','Late Payment Ratio>10_days', 'DBT', 
                         'Payment Cycle', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days', 
                         'chargeback_vol_amt', 'overdue chargebacks']


            for key1 in keys_to_change_n1:
                Customer_norm[key1] = Customer_norm[key1].apply(
                    lambda x: -x if x < 0 else x)

    # --------------------------------------------------------------------------------------------------  

        # keys_to_change_n1 = ['Unpaid Invoice (%)', 'Paid Invoice (%)', 'Open Invoices', 'Payment Timeliness Ratio (%)',
        #                      'Unpaid Invoice (%)', 'Aging Balance', 'Late Payment Ratio<10_days', 'Late Payment Ratio>10_days']

        # keys_to_change_n1 = ['Paid Invoice (%)', 'Open Invoices','Payment Timeliness Ratio (%)','Unpaid Invoice (%)','Aging Balance','Late Payment Ratio<10_days','Late Payment Ratio>10_days',
        #                  'Outstanding Balance','DBT','Open AR','chargeback_freq','chargeback_vol_amt', 'Payment Cycle']

    #        keys_to_change_n1 = ['Paid Invoice (%)', 'Payment Timeliness Ratio (%)', 
    #                     'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days','Open Invoices','Open AR',
    #                     'Aging Balance', 'Outstanding Balance','Late Payment Ratio>10_days', 'DBT', 
    #                     'Payment Cycle', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days', 
    #                     'chargeback_vol_amt', 'overdue chargebacks']


    #        for key1 in keys_to_change_n1:
    #            Customer_norm[key1] = Customer_norm[key1].apply(
    #                lambda x: -x if x < 0 else x)

        # factoring sign of DBT
        negative_DBT_check_df = Aggregated_invoice_table[Aggregated_invoice_table['Customer']==i]['DBT']

        for idx in range(len(negative_DBT_check_df)):
            if negative_DBT_check_df.values[idx] < 0:
                print('\t\t Actual DBT value -ve')
                Customer_norm['DBT'].iloc[idx] = Customer_norm['DBT'].iloc[idx] * -1 

        # calculate the weighted scores for each parameter
        weighted_scores = (Customer_norm).mul(weights, axis=1)
        map_df_list = {'customer_id': [customer_id], 'customer_name': [customer_name], 'ndg_id': [
            ndg_id], 'ndg_name': [ndg_name], 'Customer_type_id': [customer_type_id], 'customer_type': [customer_type]}
        map_df_temp = pd.DataFrame(map_df_list)

        if customer_type_id == 4:

            Aggregated_invoice_table['Score'] = round(
                ((weighted_scores.sum(axis=1)) + 57) * 2.7708333333333335)
        else:
            Aggregated_invoice_table['Score'] = round(
                ((weighted_scores.sum(axis=1)) + 3) * 133.3333)

        # replace negative values with 0
        Aggregated_invoice_table['Score'] = Aggregated_invoice_table['Score'].apply(
            lambda x: 0 if x < 0 else x)
        Aggregated_invoice_table['Score'] = Aggregated_invoice_table['Score'].apply(
            lambda x: 800 if x > 800 else x)

    # ---------------------------------------------------------------------------------------------------------
        # if empty_data_invoice_temp_flag==1:
        #     # If data_invoice_temp is empty then a random value will be assigned as score
        #     # Range is devined to keep the customer level as 1
        #     Aggregated_invoice_table['Score'] = random.randint(201,249)
    # ---------------------------------------------------------------------------------------------------------

        # assign risk levels based on customer score range
    #        Aggregated_invoice_table['Level'] = pd.cut(Aggregated_invoice_table['Score'],
    #                                                   bins=[-1, 399,
    #                                                         499, 699, 800],
    #                                                   labels=[1, 2, 3, 4])


        Aggregated_invoice_table['Level'] = pd.cut(Aggregated_invoice_table['Score'],
                                                       bins=[-1, 250,
                                                             400,800],
                                                       labels=[1, 2, 3])

        Customer = pd.concat([Customer, Aggregated_invoice_table])
        Customer_normalisation = pd.concat([Customer_normalisation, pd.concat(
            [Aggregated_invoice_table[['customer_id', 'supplier_id']], Customer_norm], axis=1)])
        map_df = pd.concat([map_df, map_df_temp])

    # In[14]:

    weight_df = pd.DataFrame(weight_list)

    # @@@@@@@@@@@ 
    # weight_df['customer_name'] = customer_list

    weight_df['customer_id'] = customer_list
    # @@@@@@@@@@@ 

    # weight_df = pd.merge(weight_df, map_df, on='customer_name', how='left')
    weight_df = pd.merge(weight_df, map_df, on='customer_id', how='left')

    # @@@@@@@@@@@ 

    # #############################
    # Customer.to_csv('./final_run/Customer.csv',header=True, index=True)
    # Customer_normalisation.to_csv('./final_run/Customer_normalisation.csv',header=True, index=True)
    # map_df.to_csv('./final_run/map_df.csv',header=True, index=True)
    # weight_df.to_csv('./final_run/weight_df.csv',header=True, index=True)



    # In[17]:

    engine = create_engine(URL(account='blb89802.us-east-1', user='RGUPTA', password='Rachana.growexx@112',
                           database='CREDIT_SCORE_PHASE2', schema='psi', warehouse='credit_score_wh', role='ACCOUNTADMIN'))
    cnn = snowflake.connector.connect(user='RGUPTA', password='Rachana.growexx@112', account='blb89802.us-east-1',
                                      warehouse='credit_score_wh', database='CREDIT_SCORE_PHASE2', schema='psi', role='ACCOUNTADMIN')
    connection = engine.connect()

    # #############################





    # Replace 'your_table' with your desired table name
    table_name = 'historical_cust_supp_map'

    historical_cust_supp_map = pd.DataFrame(historical_cust_supp_map)

    # Assuming your DataFrame is named 'df'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(f'{col} STRING' for col in historical_cust_supp_map.columns)}
    )
    """

    # Execute the query
    cnn.cursor().execute(create_table_query)


    file = table_name

    cs = cnn.cursor()

    # '/home/ubuntu/Result/output.csv'
    historical_cust_supp_map.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

    cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    """)

    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)

    cnn.commit()
    os.remove("/home/ubuntu/Result/output.csv")




    # In[18]:

    columns = [
        ('customer_id', VARCHAR(50)),
        ('previous_level', VARCHAR(50)),
        ('new_level', VARCHAR(50)),
        ('reason', VARCHAR(255)),
        ('updated_at', TIMESTAMP, text('CURRENT_TIMESTAMP()'))
    ]
    customer_update_level = pd.DataFrame(columns=[col[0] for col in columns])

    file = "customer_update_level"
    df = customer_update_level
    cs = cnn.cursor()
    cs.execute("SHOW TABLES LIKE 'customer_update_level' ")
    if not cs.fetchone():
        df.to_sql(file, if_exists='append', con=engine, index=False,
                  dtype={col[0]: col[1] for col in columns})

    columns = [
        ('supplier_id', VARCHAR(50)),
        ('previous_level', VARCHAR(50)),
        ('new_level', VARCHAR(50)),
        ('reason', VARCHAR(255)),
        ('updated_at', TIMESTAMP, text('CURRENT_TIMESTAMP()'))
    ]
    supplier_update_level = pd.DataFrame(columns=[col[0] for col in columns])

    file = "supplier_update_level"
    df = supplier_update_level
    cs = cnn.cursor()
    cs.execute("SHOW TABLES LIKE 'supplier_update_level' ")
    if not cs.fetchone():
        df.to_sql(file, if_exists='append', con=engine, index=False,
                  dtype={col[0]: col[1] for col in columns})

    data = {'reason': ['Length of relationship with PSI',
                       'Credit Risk in credit Bureaus',
                       'Located in a region with the highest credit risk',
                       'Years of experience that the company has in the alcoholic beverage industry',
                       'Significant changes or developments in the business',
                       'The company is currently undergoing bankruptcy proceedings',
                       'External references',
                       'Solid company - Solid/consolidated management',
                       'If the company has a history of overdue/delinquency within the PSI, they were able to plausibly justify the overdue',
                       'The company belongs to the same economic group as another company with a low score and/or is delinquent',
                       'High level of rejected chargebacks',
                       'Customer dependency (more concentrated or more diversified)',
                       'Level of trust',
                       'explanation for the values to be set'
                       ]}
    reason_df = pd.DataFrame(data)


    supplier_reason = pd.merge(crm_classes[['supplier_id']].assign(
        key=1), reason_df.assign(key=1), on='key').drop('key', axis=1)
    # supplier_reason

    engine = create_engine(URL(account='blb89802.us-east-1', user='RGUPTA', password='Rachana.growexx@112',
                       database='CREDIT_SCORE_PHASE2', schema='psi', warehouse='credit_score_wh', role='ACCOUNTADMIN'))
    cnn = snowflake.connector.connect(user='RGUPTA', password='Rachana.growexx@112', account='blb89802.us-east-1',
                                      warehouse='credit_score_wh', database='CREDIT_SCORE_PHASE2', schema='psi', role='ACCOUNTADMIN')
    connection = engine.connect()

    query = ("select * from supplier_reason")
    snowflake_supplier_reason = pd.read_sql(query, cnn)

    # Assuming your local DataFrame is named 'local_df'
    missing_rows = supplier_reason[~supplier_reason['supplier_id'].isin(snowflake_supplier_reason['supplier_id'.upper()])]
    missing_rows['weight'] = 0.1
    missing_rows.rename(columns={'supplier_id':'SUPPLIER_ID','reason':'REASON', 'weight':'WEIGHT'},inplace=True)
    print('### Missing rows in supplier_reason ',len(missing_rows))
    print(missing_rows['SUPPLIER_ID'].unique())

    # print("Missing rows ", len(missing_rows))
    print("Supplier count ", len(missing_rows['SUPPLIER_ID'].unique()))

    cs = cnn.cursor()
    file = 'supplier_reason'

    # '/home/ubuntu/Result/output.csv'
    missing_rows.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

    cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """ OVERWRITE=TRUE
    """)

    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)

    cnn.commit()
    os.remove("/home/ubuntu/Result/output.csv")

    # ----------------------------------------------------------------------------------------------------------------------
    # supplier_reason['weight'] = 0.1
    # file = "supplier_reason"
    # file1 = '"supplier_reason"'

    # cs = cnn.cursor()
    # cs.execute("""TRUNCATE TABLE IF EXISTS """ + file + """""")

    # # '/home/ubuntu/Result/output.csv'
    # supplier_reason.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

    # cs.execute("""
    #     PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """ OVERWRITE=TRUE
    # """)

    # cs.execute("""
    #     COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
    #     FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #     FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    # """)

    # cnn.commit()
    # os.remove("/home/ubuntu/Result/output.csv")

    # cs = cnn.cursor()
    # cs.execute("SHOW TABLES LIKE 'supplier_reason' ")
    # if not cs.fetchone():
    #     df = supplier_reason
    #     i = 10
    #     df1 = df.head(i)
    #     df1.to_sql(file, if_exists='append', con=engine, index=False)

    #     if i <= len(df):
    #         rest_of_df = df.iloc[i:]
    #         rest_of_df.to_csv(
    #             '/home/ubuntu/Result/output.csv', index=False)

    #         cs.execute("""
    #         PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #     """)
    #     cs.execute("""
    #         COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
    #         FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #         FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    #     """)
    #     cnn.commit()
    #     os.remove('/home/ubuntu/Result/output.csv')

    engine = create_engine(URL(account='blb89802.us-east-1', user='RGUPTA', password='Rachana.growexx@112',
                       database='CREDIT_SCORE_PHASE2', schema='psi', warehouse='credit_score_wh', role='ACCOUNTADMIN'))
    cnn = snowflake.connector.connect(user='RGUPTA', password='Rachana.growexx@112', account='blb89802.us-east-1',
                                      warehouse='credit_score_wh', database='CREDIT_SCORE_PHASE2', schema='psi', role='ACCOUNTADMIN')
    connection = engine.connect()

    # customer_reason

    customer_reason = pd.DataFrame()
    customer_reason = pd.merge(crm_customers[['customer_id']].assign(
        key=1), reason_df.assign(key=1), on='key').drop('key', axis=1)

    query = ("select * from customer_reason")
    snowflake_customer_reason = pd.read_sql(query, cnn)

    # Assuming your local DataFrame is named 'local_df'
    missing_rows = customer_reason[~customer_reason['customer_id'].isin(snowflake_customer_reason['customer_id'.upper()])]
    missing_rows['weight'] = 0.1
    missing_rows.rename(columns={'customer_id':'CUSTOMER_ID','reason':'REASON', 'weight':'WEIGHT'},inplace=True)
    print('### Missing rows in customer_reason ',len(missing_rows))
    print(missing_rows['CUSTOMER_ID'].unique())

    # print("Missing rows ", len(missing_rows))
    print("Customer count ", len(missing_rows['CUSTOMER_ID'].unique()))

    cs = cnn.cursor()
    file = 'customer_reason'

    # '/home/ubuntu/Result/output.csv'
    missing_rows.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

    cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """ OVERWRITE=TRUE
    """)

    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)

    cnn.commit()
    os.remove("/home/ubuntu/Result/output.csv")


    # --------------------------------------------------------------------------------------------------
    # customer_reason['weight'] = 0.1
    # file = "customer_reason"
    # file1 = '"customer_reason"'


    # cs = cnn.cursor()
    # cs.execute("""TRUNCATE TABLE IF EXISTS """ + file + """""")

    # # '/home/ubuntu/Result/output.csv'
    # customer_reason.to_csv("/home/ubuntu/Result/output.csv", header=True, index=False)

    # cs.execute("""
    #     PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """ OVERWRITE=TRUE
    # """)

    # cs.execute("""
    #     COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
    #     FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #     FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    # """)

    # cnn.commit()
    # os.remove("/home/ubuntu/Result/output.csv")

    # cs = cnn.cursor()
    # cs.execute("SHOW TABLES LIKE 'customer_reason' ")
    # if not cs.fetchone():
    #     df = customer_reason
    #     i = 10
    #     df1 = df.head(i)
    #     df1.to_sql(file, if_exists='append', con=engine, index=False)

    #     if i <= len(df):
    #         rest_of_df = df.iloc[i:]
    #         rest_of_df.to_csv(
    #             '/home/ubuntu/Result/output.csv', index=False)

    #         cs.execute("""
    #         PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #     """)
    #     cs.execute("""
    #         COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file + """
    #         FROM @CREDIT_SCORE_PHASE2.psi.%""" + file + """
    #         FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    #     """)
    #     cnn.commit()
    #     os.remove('/home/ubuntu/Result/output.csv')


    # # Predictor Fields

    # Predicting Amount < 10 days

    # In[21]:

    mydate10 = my_date + timedelta(days=9)

    # @@@@@@@@@@@@@@@@@@@@@@@ customer_id was not included here, added it.
    Predict_data10 = Data_processed[(Data_processed['duedate']-my_date > timedelta(days=-10)) & (Data_processed['duedate']-my_date <= timedelta(days=10)) & (Data_processed['ispaid'] == 0)][[
        'customer_id','invoice_num', 'supplier_id' ,'client_name', 'customer_name', 'ndg_name', 'duedate', 'invoice_amount', 'invoice_date', 'txn_date', 'txn_amount', 'customer_type']]

    # @@@@@@@@@@@@@@@@@@@@@@@

    # p_customer_list10 = Predict_data10['customer_name'].unique().tolist()

    p_customer_list10 = Predict_data10['customer_id'].unique().tolist()


    # @@@@@@@@@@@@@@@@@@@@@@@


    Aggregated_predict_table10 = pd.DataFrame()
    count_p = 1

    for i in p_customer_list10:
        
        # @@@@@@@@@@@@@@@@@@@@@@@
    #     p_supplier_list = Predict_data10[Predict_data10['customer_name']
    #                                      == i]['client_name'].unique().tolist()
        
        
        p_supplier_list = Predict_data10[Predict_data10['customer_id']
                                         == i]['supplier_id'].unique().tolist()
        
        # @@@@@@@@@@@@@@@@@@@@@@@
        for j in p_supplier_list:
            predictor_df = pd.DataFrame()
            
            # @@@@@@@@@@@@@@@@@@@@@@@
    #         Predict_data_temp = Predict_data10[(Predict_data10['customer_name'] == i) & (
    #             Predict_data10['client_name'] == j)]

    #         early_payment = abs(Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['early_payment_perc'].values[0])
    #         late_payment_before_10 = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['payment1_percent'].values[0]
    #         late_payment_after_10 = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['payment2_percent'].values[0]

    #         early_payment_date_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['early_payment_days']
    #         late_payment_date_before_10_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['Delinquent Overdueday<10_days']
    #         late_payment_date_after_10_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['Delinquent Overdueday>10_days']

            Predict_data_temp = Predict_data10[(Predict_data10['customer_id'] == i) & (
                Predict_data10['supplier_id'] == j)]

            early_payment = abs(Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['early_payment_perc'].values[0])
            
            late_payment_before_10 = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['payment1_percent'].values[0]
            
            late_payment_after_10 = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['payment2_percent'].values[0]

            early_payment_date_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['early_payment_days']
            
            late_payment_date_before_10_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['Delinquent Overdueday<10_days']
            
            late_payment_date_after_10_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['Delinquent Overdueday>10_days']
            
            # @@@@@@@@@@@@@@@@@@@@@@@

            predictor_df = Predict_data_temp.groupby(['invoice_num'], as_index=False).agg({'customer_id':'first',
                                                                                           'supplier_id':'first',
                                                                                           'client_name': 'first',
                                                                                           'customer_name': 'first',
                                                                                           'ndg_name': 'first',
                                                                                           'customer_type': 'first',
                                                                                           'duedate': 'first',
                                                                                           'invoice_amount': 'first',
                                                                                           'invoice_date': 'first'})
            default_date = date(1900, 1, 1)

            invoice_list = predictor_df['invoice_num'].unique().tolist()

            for k in invoice_list:
                predictor_df.loc[predictor_df['invoice_num'] == k, 'txn_amount'] = Predict_data_temp[(
                    Predict_data_temp['txn_date'].fillna(default_date) <= my_date) & (Predict_data_temp['invoice_num'] == k)]['txn_amount'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment'] = (
                    early_payment*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment_date'] = predictor_df[predictor_df['invoice_num']
                                                                                                        == k]['duedate'] - timedelta(days=int(early_payment_date_temp))
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment<10'] = (
                    late_payment_before_10*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'late_payment_date_before_10'] = predictor_df[predictor_df['invoice_num']
                                                                                                                 == k]['duedate'] + timedelta(days=int(late_payment_date_before_10_temp))
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment>10'] = (
                    late_payment_after_10*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'late_payment_date_after_10'] = predictor_df[predictor_df['invoice_num']
                                                                                                                == k]['duedate'] + timedelta(days=int(late_payment_date_after_10_temp))

                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_early'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['early_payment_date'] <= mydate10), 'early_payment'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_late_before_10'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['late_payment_date_before_10'] <= mydate10), 'early_payment<10'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_late_after_10'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['late_payment_date_after_10'] <= mydate10), 'early_payment>10'].sum()

                predictor_df.loc[predictor_df['invoice_num'] == k, 'Predict_AR < 10days'] = (predictor_df[predictor_df['invoice_num'] == k]['predictor_early'] +
                                                                                             predictor_df[predictor_df['invoice_num'] == k]['predictor_late_before_10'] +
                                                                                             predictor_df[predictor_df['invoice_num'] == k]['predictor_late_after_10']) - abs(predictor_df[predictor_df['invoice_num'] == k]['txn_amount'])

                if (predictor_df[predictor_df['invoice_num'] == k]['Predict_AR < 10days'] < 0).any():
                    predictor_df.loc[(predictor_df['invoice_num'] == k) & (
                        predictor_df['Predict_AR < 10days'] < 0), 'Predict_AR < 10days'] = 0

            if count_p == 1:
                Aggregated_predict_table10 = predictor_df
            else:
                Aggregated_predict_table10 = pd.concat(
                    [Aggregated_predict_table10, predictor_df])
            count_p = count_p + 1



    if len(Aggregated_predict_table10) != 0:
        
        # @@@@@@@@@@@@@@@@@@@@@@@
        # Aggregated_predict_table10_group = Aggregated_predict_table10.groupby(
        #     ['customer_name', 'client_name'], as_index=False).agg({'Predict_AR < 10days': 'sum'})

        Aggregated_predict_table10_group = Aggregated_predict_table10.groupby(
        ['customer_id', 'supplier_id'], as_index=False).agg({'Predict_AR < 10days': 'sum'})

        # @@@@@@@@@@@@@@@@@@@@@@@

        Aggregated_predict_table10_group.rename(
            columns={'customer_name': 'Customer', 'client_name': 'supplier'}, inplace=True)
        
        if len(Aggregated_predict_table10_group) != 0:

            # @@@@@@@@@@@@@@@@@@@@@@@
        #     Customer_rating = pd.merge(Customer, Aggregated_predict_table10_group, on=[
        #                                'Customer', 'supplier'], how='left')

            Customer_rating = pd.merge(Customer, Aggregated_predict_table10_group, on=[
                                       'customer_id', 'supplier_id'], how='left')

            # @@@@@@@@@@@@@@@@@@@@@@@


            Customer_rating['Predict_AR < 10days'] = Customer_rating[[
                'Predict_AR < 10days']].fillna(0)
        else:
            Customer_rating['Predict_AR < 10days'] = 0
            
    else:
        Customer_rating = Customer.copy()
        Customer_rating['Predict_AR < 10days'] = 0
        
        
    # Predicting Amount < 30 days

    # In[23]:

    mydate30 = my_date + timedelta(days=29)

    # @@@@@@@@@@@@@@@@@@@@@@@ customeer_id, client_id was not included here, added it.
    Predict_data30 = Data_processed[(Data_processed['duedate']-my_date > timedelta(days=-10)) & (Data_processed['duedate']-my_date <= timedelta(days=30)) & (Data_processed['ispaid'] == 0)][[
        'customer_id','invoice_num', 'supplier_id' ,'client_name', 'customer_name', 'ndg_name', 'duedate', 'invoice_amount', 'invoice_date', 'txn_date', 'txn_amount', 'customer_type']]

    # @@@@@@@@@@@@@@@@@@@@@@@
    # p_customer_list30 = Predict_data30['customer_name'].unique().tolist()

    p_customer_list30 = Predict_data30['customer_id'].unique().tolist()

    # @@@@@@@@@@@@@@@@@@@@@@@


    Aggregated_predict_table30 = pd.DataFrame()
    count_p = 1
    for i in p_customer_list30:
        
        # @@@@@@@@@@@@@@@@@@@@@@@
    #     p_supplier_list = Predict_data30[Predict_data30['customer_name']
    #                                      == i]['client_name'].unique().tolist()

        p_supplier_list = Predict_data30[Predict_data30['customer_id']
                                         == i]['supplier_id'].unique().tolist()
        
        # @@@@@@@@@@@@@@@@@@@@@@@
        for j in p_supplier_list:
            predictor_df = pd.DataFrame()
            
            # @@@@@@@@@@@@@@@@@@@@@@@
    #         Predict_data_temp = Predict_data30[(Predict_data30['customer_name'] == i) & (
    #             Predict_data30['client_name'] == j)]

    #         early_payment = abs(Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['early_payment_perc'].iloc[0])
    #         late_payment_before_10 = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['payment1_percent'].iloc[0]
    #         late_payment_after_10 = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['payment2_percent'].iloc[0]

    #         early_payment_date_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['early_payment_days']
    #         late_payment_date_before_10_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['Delinquent Overdueday<10_days']
    #         late_payment_date_after_10_temp = Customer[(Customer['Customer'] == i) & (
    #             Customer['supplier'] == j)]['Delinquent Overdueday>10_days']

            Predict_data_temp = Predict_data30[(Predict_data30['customer_id'] == i) & (
                Predict_data30['supplier_id'] == j)]

            early_payment = abs(Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['early_payment_perc'].iloc[0])
            
            late_payment_before_10 = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['payment1_percent'].iloc[0]
            
            late_payment_after_10 = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['payment2_percent'].iloc[0]

            
            early_payment_date_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['early_payment_days']
            
            late_payment_date_before_10_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['Delinquent Overdueday<10_days']
            
            late_payment_date_after_10_temp = Customer[(Customer['customer_id'] == i) & (
                Customer['supplier_id'] == j)]['Delinquent Overdueday>10_days']
            
            # @@@@@@@@@@@@@@@@@@@@@@@

            predictor_df = Predict_data_temp.groupby(['invoice_num'], as_index=False).agg({'customer_id':'first',
                                                                                           'supplier_id':'first',
                                                                                           'client_name': 'first',
                                                                                           'customer_name': 'first',
                                                                                           'ndg_name': 'first',
                                                                                           'customer_type': 'first',
                                                                                           'duedate': 'first',
                                                                                           'invoice_amount': 'first',
                                                                                           'invoice_date': 'first'})
            default_date = date(1900, 1, 1)

            invoice_list = predictor_df['invoice_num'].unique().tolist()

            for k in invoice_list:
                predictor_df.loc[predictor_df['invoice_num'] == k, 'txn_amount'] = Predict_data_temp[(
                    Predict_data_temp['txn_date'].fillna(default_date) <= my_date) & (Predict_data_temp['invoice_num'] == k)]['txn_amount'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment'] = (
                    early_payment*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment_date'] = predictor_df[predictor_df['invoice_num']
                                                                                                        == k]['duedate'] - timedelta(days=int(early_payment_date_temp))
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment<10'] = (
                    late_payment_before_10*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'late_payment_date_before_10'] = predictor_df[predictor_df['invoice_num']
                                                                                                                 == k]['duedate'] + timedelta(days=int(late_payment_date_before_10_temp))
                predictor_df.loc[predictor_df['invoice_num'] == k, 'early_payment>10'] = (
                    late_payment_after_10*predictor_df[predictor_df['invoice_num'] == k]['invoice_amount'])/100
                predictor_df.loc[predictor_df['invoice_num'] == k, 'late_payment_date_after_10'] = predictor_df[predictor_df['invoice_num']
                                                                                                                == k]['duedate'] + timedelta(days=int(late_payment_date_after_10_temp))

                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_early'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['early_payment_date'] <= mydate30), 'early_payment'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_late_before_10'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['late_payment_date_before_10'] <= mydate30), 'early_payment<10'].sum()
                predictor_df.loc[predictor_df['invoice_num'] == k, 'predictor_late_after_10'] = predictor_df.loc[(
                    predictor_df['invoice_num'] == k) & (predictor_df['late_payment_date_after_10'] <= mydate30), 'early_payment>10'].sum()

                predictor_df.loc[predictor_df['invoice_num'] == k, 'Predict_AR < 30days'] = (predictor_df[predictor_df['invoice_num'] == k]['predictor_early'] +
                                                                                             predictor_df[predictor_df['invoice_num'] == k]['predictor_late_before_10'] +
                                                                                             predictor_df[predictor_df['invoice_num'] == k]['predictor_late_after_10']) - abs(predictor_df[predictor_df['invoice_num'] == k]['txn_amount'])

                if (predictor_df[predictor_df['invoice_num'] == k]['Predict_AR < 30days'] < 0).any():
                    predictor_df.loc[(predictor_df['invoice_num'] == k) & (
                        predictor_df['Predict_AR < 30days'] < 0), 'Predict_AR < 30days'] = 0

            if count_p == 1:
                Aggregated_predict_table30 = predictor_df
            else:
                Aggregated_predict_table30 = pd.concat(
                    [Aggregated_predict_table30, predictor_df])
            count_p = count_p + 1


    # @@@@@@@@@@@@@@@@@@@@@@@
    # Aggregated_predict_table30_group = Aggregated_predict_table30.groupby(
    #     ['customer_name', 'client_name'], as_index=False).agg({'Predict_AR < 30days': 'sum'})


    if len(Aggregated_predict_table30) != 0:

        Aggregated_predict_table30_group = Aggregated_predict_table30.groupby(
            ['customer_id', 'supplier_id'], as_index=False).agg({'Predict_AR < 30days': 'sum'})

        # @@@@@@@@@@@@@@@@@@@@@@@

        Aggregated_predict_table30_group.rename(
            columns={'customer_name': 'Customer', 'client_name': 'supplier'}, inplace=True)


        if len(Aggregated_predict_table30_group) != 0:

            # @@@@@@@@@@@@@@@@@@@@@@@
        #     Customer_rating = pd.merge(Customer_rating, Aggregated_predict_table30_group, on=[
        #                                'Customer', 'supplier'], how='left')

            Customer_rating = pd.merge(Customer_rating, Aggregated_predict_table30_group, on=[
                                       'customer_id', 'supplier_id'], how='left')

            # @@@@@@@@@@@@@@@@@@@@@@@
            Customer_rating['Predict_AR < 30days'] = Customer_rating[[
                'Predict_AR < 30days']].fillna(0)
        else:
            Customer_rating['Predict_AR < 30days'] = 0
            
    else:
        Customer_rating = pd.merge(Customer_rating, Aggregated_predict_table30_group, on=[
                               'customer_id', 'supplier_id'], how='left')
        Customer_rating['Predict_AR < 30days'] = 0


    #
    # # Supplier Credit Score

    # In[24]:

    customer_report = Customer_rating
    customer_report = customer_report[['customer_id', 'supplier_id', 'Customer', 'supplier', 'Open Invoices',
                                       'Open AR', 'total_AR', 'Outstanding Balance', 'Predict_AR < 10days', 'Predict_AR < 30days', 'Score']]
    customer_report = customer_report[customer_report['supplier']
                                      != 'Matchless']

    # @@@@@@@@@@@@@@@@@@@@@@@
    # customer_report1 = customer_report.groupby(['supplier'], as_index=False).agg({'Open Invoices': 'sum',
    #                                                                              'Open AR': 'sum',
    #                                                                               'total_AR': 'sum',
    #                                                                               'Outstanding Balance': 'sum',
    #                                                                               'Predict_AR < 10days': 'sum',
    #                                                                               'Predict_AR < 30days': 'sum',
    #                                                                               'Score': 'mean'})

    customer_report1 = customer_report.groupby(['supplier_id'], as_index=False).agg({'Open Invoices': 'sum',
                                                                                 'Open AR': 'sum',
                                                                                  'total_AR': 'sum',
                                                                                  'Outstanding Balance': 'sum',
                                                                                  'Predict_AR < 10days': 'sum',
                                                                                  'Predict_AR < 30days': 'sum',
                                                                                  'Score': 'mean'})

    # @@@@@@@@@@@@@@@@@@@@@@@

    customer_report.rename(columns={'Score': 'customer_score'}, inplace=True)


    # customer_report = pd.merge(customer_report, contract_status, on=[
    #                            'supplier'], how='left')

    customer_report = pd.merge(customer_report, contract_status, on=[
                               'supplier_id'], how='left')

    customer_report1.rename(columns={'Score': 'customer_score'}, inplace=True)


    # customer_report1 = pd.merge(customer_report1, contract_status, on=[
    #                             'supplier'], how='left')


    customer_report1 = pd.merge(customer_report1, contract_status, on=[
                                'supplier_id'], how='left')

    # @@@@@@@@@@@@@@@@@@@@@@@
    # In[25]:


    Supplier = pd.DataFrame()
    Supplier1 = pd.DataFrame()
    Supplier_data = pd.DataFrame()


    # @@@@@@@@@@@@@@@@@@@@@@@

    # Supplier = pd.merge(customer_report, open_payable, on=['supplier','display_name','status_short'], how='left')
    # Supplier = pd.merge(Supplier, credit_memo, on=['supplier','display_name','status_short'], how='left')
    # Supplier = pd.merge(Supplier, other_open_payables, on=['supplier','display_name','status_short'], how='left')
    # Supplier = pd.merge(Supplier, cash_balance, on=['supplier','display_name','status_short'], how='left')
    # Supplier = pd.merge(Supplier, unapplied_balance, on=['supplier','display_name','status_short'], how='left')


    Supplier = pd.merge(customer_report, open_payable, left_on='supplier_id', right_on='id',how='left')

    Supplier.drop(columns=['supplier_y', 'status_short_y','display_name_y','id'], inplace=True)
    Supplier.rename(columns={'supplier_x':'supplier','status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)

    Supplier = pd.merge(Supplier, credit_memo, left_on='supplier_id', right_on='supplier_id',how='left')

    Supplier.drop(columns=['status_short_y','display_name_y'], inplace=True)
    Supplier.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)

    Supplier = pd.merge(Supplier, other_open_payables, left_on='supplier_id', right_on='client_id',how='left')

    Supplier.drop(columns=['client_id'], inplace=True)

    Supplier = pd.merge(Supplier, cash_balance, left_on='supplier_id', right_on='id',how='left')


    Supplier.drop(columns=['supplier_y', 'status_short_y','display_name_y','id'], inplace=True)
    Supplier.rename(columns={'supplier_x':'supplier','status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)

    Supplier = pd.merge(Supplier, unapplied_balance, left_on='supplier_id', right_on='id',how='left')


    Supplier.drop(columns=['status_short_y','display_name_y','id'], inplace=True)
    Supplier.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)

    # @@@@@@@@@@@@@@@@@@@@@@@


    Supplier= Supplier.fillna(0)
    Supplier['wallet'] = Supplier['unapplied_amount'] - Supplier['other_open_payables'] - Supplier['open_credit_memo']
    # Supplier['Risk_Balance'] = (Supplier['total_AR']-Supplier['other_open_payables']-Supplier['open_credit_memo'])+Supplier['cash_balance']
    Supplier['Risk_Balance'] = (Supplier['Open AR']-Supplier['other_open_payables']-Supplier['open_credit_memo'])+Supplier['cash_balance']


    Supplier= Supplier.reindex(columns=['supplier_id','supplier','display_name','status_short', 'Open Invoices', 'Open AR', 'unapplied_amount','open_payable','open_credit_memo', 'other_open_payables', 'cash_balance','wallet','Risk_Balance', 'Predict_AR < 10days',
                                        'Predict_AR < 30days', 'customer_score','customer_id','Customer', 'total_AR','Outstanding Balance'])
    Supplier[['open_payable', 'open_credit_memo',
              'other_open_payables', 'cash_balance', 'unapplied_amount']]= Supplier[['open_payable', 'open_credit_memo',
                                                                                     'other_open_payables', 'cash_balance', 'unapplied_amount']].fillna(0)


    # @@@@@@@@@@@@@@@@@@@@@@@


    # Supplier1 = pd.merge(customer_report1, open_payable, on=['supplier','display_name','status_short'], how='left')
    # Supplier1 = pd.merge(Supplier1, credit_memo, on=['supplier','display_name','status_short'], how='left')
    # Supplier1 = pd.merge(Supplier1, other_open_payables, on=['supplier','display_name','status_short'], how='left')
    # Supplier1 = pd.merge(Supplier1, cash_balance, on=['supplier','display_name','status_short'], how='left')
    # Supplier1 = pd.merge(Supplier1, unapplied_balance, on=['supplier','display_name','status_short'], how='left')

    # Supplier1 = pd.merge(customer_report1, open_payable, left_on='supplier_id', right_on='id',how='left')
    # Supplier1 = pd.merge(Supplier1, credit_memo, left_on='supplier_id', right_on='supplier_id',how='left')
    # Supplier1 = pd.merge(Supplier1, other_open_payables, left_on='supplier_id', right_on='client_id',how='left')
    # Supplier1 = pd.merge(Supplier1, cash_balance, left_on='supplier_id', right_on='id',how='left')
    # Supplier1 = pd.merge(Supplier1, unapplied_balance, left_on='supplier_id', right_on='id',how='left')

    Supplier1 = pd.DataFrame()

    Supplier1 = pd.merge(customer_report1, open_payable, left_on='supplier_id', right_on='id',how='left')


    Supplier1.drop(columns=['status_short_y','display_name_y','id'], inplace=True)
    Supplier1.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    Supplier1 = pd.merge(Supplier1, credit_memo, left_on='supplier_id', right_on='supplier_id',how='left')


    Supplier1.drop(columns=['status_short_y','display_name_y'], inplace=True)
    Supplier1.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    Supplier1 = pd.merge(Supplier1, other_open_payables, left_on='supplier_id', right_on='client_id',how='left')


    Supplier1.drop(columns=['client_id'], inplace=True)


    Supplier1 = pd.merge(Supplier1, cash_balance, left_on='supplier_id', right_on='id',how='left')


    Supplier1.drop(columns=['supplier_y', 'status_short_y','display_name_y','id'], inplace=True)
    Supplier1.rename(columns={'supplier_x':'supplier','status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    Supplier1 = pd.merge(Supplier1, unapplied_balance, left_on='supplier_id', right_on='id',how='left')


    Supplier1.drop(columns=['status_short_y','display_name_y','id'], inplace=True)
    Supplier1.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)





    # @@@@@@@@@@@@@@@@@@@@@@@


    Supplier1[['open_payable', 'open_credit_memo',
               'other_open_payables', 'cash_balance', 'unapplied_amount']]= Supplier1[['open_payable', 'open_credit_memo',
                                                                                       'other_open_payables', 'cash_balance', 'unapplied_amount']].fillna(0)
    Supplier1['wallet'] = Supplier1['unapplied_amount'] - Supplier1['other_open_payables']- Supplier1['open_credit_memo']

    # Supplier1['Risk_Balance'] = (Supplier1['total_AR']-Supplier1['other_open_payables']-Supplier1['open_credit_memo'])+Supplier1['cash_balance']
    Supplier1['Risk_Balance'] = (Supplier1['Open AR']-Supplier1['other_open_payables']-Supplier1['open_credit_memo'])+Supplier1['cash_balance']

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    Supplier1= Supplier1.reindex(columns=['supplier_id','supplier','display_name','status_short', 'Open Invoices', 'total_AR','unapplied_amount', 'open_payable','open_credit_memo', 'other_open_payables', 'cash_balance','wallet','Risk_Balance', 'Predict_AR < 10days',
                                          'Predict_AR < 30days', 'customer_score'])
    Supplier_metric = Supplier1.drop(columns=['supplier_id','supplier','display_name','status_short','Predict_AR < 10days','Predict_AR < 30days'])
    from sklearn.decomposition import PCA
    data_z = (Supplier_metric - Supplier_metric.mean()) / \
        (Supplier_metric.std())
    data_z = data_z.fillna(0)
    # Perform PCA using scikit-learn
    pca = PCA()
    pca.fit(data_z)

    # Get the loadings (weights) for each variable
    loadings = pd.DataFrame(pca.components_.T, columns=[
                            'PC1', 'PC2', 'PC3', 'PC4', 'PC5', 'PC6', 'PC7', 'PC8', 'PC9', 'PC10'], index=Supplier_metric.columns)
    weights = loadings['PC1'].to_dict()
    # keys_to_change_n = ['open_payable',
    #                     'open_credit_memo', 'other_open_payables']
    # keys_to_change_p = ['Open Invoices', 'total_AR',
    #                     'cash_balance', 'Risk_Balance', 'customer_score', 'wallet']

    keys_to_change_n = ['open_payable',
                    'open_credit_memo', 'other_open_payables']

    keys_to_change_p = ['Open Invoices', 'total_AR',
                        'Risk_Balance', 'wallet', 'unapplied_amount']

    for key in keys_to_change_n:
        if weights[key] > 0:
            weights[key] *= -1
    for key in keys_to_change_p:
        if weights[key] < 0:
            weights[key] *= -1

    # In[28]:

    supplier_weight_df_temp = pd.DataFrame(weights, index=[0])
    supplier_weight_df = pd.DataFrame()

    # @@@@@@@@@@@@@@

    # supplier_weight_df['supplier'] = Supplier1['supplier'].unique().tolist()
    # supplier_weight_df = pd.merge(supplier_weight_df.assign(
    #    key=1), supplier_weight_df_temp.assign(key=1), on='key').drop('key', axis=1)

    supplier_weight_df['supplier_id'] = Supplier1['supplier_id'].unique().tolist()

    supplier_weight_df = pd.merge(supplier_weight_df.assign(
        key=1), supplier_weight_df_temp.assign(key=1), on='key').drop('key', axis=1)

    # @@@@@@@@@@@@@@    


    Supplier_data = Supplier.drop(columns=['supplier_id','customer_id','Customer','Open AR','Outstanding Balance','supplier','display_name','status_short','Predict_AR < 10days','Predict_AR < 30days'])


    Supplier_data = Supplier_data.reindex(columns=['Open Invoices', 'total_AR', 'unapplied_amount', 'open_payable',
                                                   'open_credit_memo', 'other_open_payables', 'cash_balance', 'wallet',
                                                   'Risk_Balance', 'customer_score'])

    #    Supplier_norm = (Supplier_data - Supplier_metric.mean()) / \
    #        (Supplier_metric.std())
    #    Supplier_norm.fillna(0, inplace=True)

    keys_to_change_n = ['open_payable',
                    'open_credit_memo', 'other_open_payables']

    keys_to_change_p = ['Open Invoices', 'total_AR',
                        'Risk_Balance', 'wallet', 'unapplied_amount']

    Supplier_norm_data = []

    min_max_constant=0.01

    for supp_id in Supplier['supplier_id'].unique():
    #     supp_id = 1985
        print(supp_id)

        all_data = Supplier[Supplier['supplier_id']==supp_id][['Open Invoices', 'total_AR', 'unapplied_amount', 'open_payable',
           'open_credit_memo', 'other_open_payables', 'cash_balance', 'wallet','Risk_Balance', 'customer_score']]

        norm_data = (all_data - all_data.mean())/ all_data.std()

        if len(all_data)!= 1:
            for row in norm_data.columns:
                for idx in range(len(norm_data)):
                    if all_data[row].values[idx]== 0.0:
                        norm_data[row].values[idx] = 0

            norm_data_max_min = ((norm_data - norm_data.min()) + min_max_constant) / ((norm_data.max() - norm_data.min()) + min_max_constant)
            norm_data_max_min.fillna(0, inplace=True)

            norm_data = norm_data_max_min

            for row in norm_data.columns:
                for idx in range(len(norm_data)):
                    if all_data[row].values[idx]== 0.0:
                        norm_data[row].values[idx] = 0



        else:

            print("Calculating new Supplier norm values ....")


            positive_impact_param_mean = all_data[keys_to_change_p].values[0].mean()

            positive_impact_param_std = all_data[keys_to_change_p].values[0].std()


            for key in keys_to_change_p:
                norm_data[key] = (all_data[key] - positive_impact_param_mean)/positive_impact_param_std

            negative_impact_param_mean = all_data[keys_to_change_n].values[0].mean()
            negative_impact_param_std = all_data[keys_to_change_n].values[0].std()

            for key in keys_to_change_n:
                norm_data[key] = (all_data[key] - negative_impact_param_mean)/negative_impact_param_std

            for row in norm_data.columns:
                for idx in range(len(norm_data)):
                    if all_data[row].values[idx]== 0.0:
                        norm_data[row].values[idx] = 0

            keys_to_change_n1 = ['open_payable',
                    'open_credit_memo', 'other_open_payables', 'Open Invoices', 'total_AR',
                    'Risk_Balance', 'wallet', 'unapplied_amount']

            for key1 in keys_to_change_n1:
                norm_data[key1] = norm_data[key1].apply(
                    lambda x: -x if x < 0 else x)




        Supplier_norm_data.append(norm_data)

    Supplier_norm = pd.concat(Supplier_norm_data,ignore_index=False)
    Supplier_norm = Supplier_norm.sort_index()
    Supplier_norm.fillna(0, inplace=True)
    Supplier_norm


    #    keys_to_change_n1 = ['open_payable',
    #                    'open_credit_memo', 'other_open_payables', 'Open Invoices', 'total_AR',
    #                    'Risk_Balance', 'wallet', 'unapplied_amount']

    #    for key1 in keys_to_change_n1:
    #        Supplier_norm[key1] = Supplier_norm[key1].apply(
    #            lambda x: -x if x < 0 else x)


    weighted_scores = Supplier_norm.mul(weights, axis=1)

    # Drop customer score
    #    weighted_scores.drop(columns=['customer_score'], inplace=True)

    #    Supplier['Score'] = round(((weighted_scores.sum(axis=1)+10)/20)*800)
    Supplier['Score'] = round(((weighted_scores.sum(axis=1)+3))*133.33)


    # replace negative values with 0
    Supplier['Score'] = Supplier['Score'].apply(lambda x: 0 if x < 0 else x)
    Supplier['Score'] = Supplier['Score'].apply(
        lambda x: 800 if x > 800 else x)

    # assign risk levels based on customer score range
    #    Supplier['Level'] = pd.cut(Supplier['Score'],
    #                               bins=[-1, 399, 499, 699, 800],
    #                               labels=[1, 2, 3, 4])


    Supplier['Level'] = pd.cut(Supplier['Score'],
                               bins=[-1, 250,400,800],
                               labels=[1, 2, 3])

    # In[30]:

    Risk_factor_data_temp = pd.DataFrame()
    Risk_factor_data_temp = Supplier_metric.mul(weights, axis=1)
    Risk_factor_data_temp = Risk_factor_data_temp.drop(
        columns=['Risk_Balance', 'customer_score'])

    series1 = pd.Series(Risk_factor_data_temp.apply(lambda row: row[row <= 0].abs(
    ).nlargest(2).index.tolist() if any(row <= 0) else [], axis=1))
    series2 = pd.Series(Risk_factor_data_temp.apply(lambda row: row[row > 0].abs(
    ).nsmallest(1).index.tolist() if any(row > 0) else [], axis=1))
    Risk_factor_data_temp['Risk_Factor'] = pd.Series(
        [list(pair) for pair in zip(series1, series2)])
    Risk_factor_data_temp['Risk_Factor'] = Risk_factor_data_temp['Risk_Factor'].apply(
        lambda x: sum(x, []))
    Risk_factor_data_temp['Risk_Factor'] = Risk_factor_data_temp['Risk_Factor'].apply(
        lambda x: ', '.join(x))

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    # Risk_factor_data = pd.DataFrame()

    # Risk_factor_data['supplier'] = Supplier1['supplier'].unique().tolist()

    # Risk_factor_data = pd.concat(
    #     [Risk_factor_data, Risk_factor_data_temp], axis=1)

    # Supplier = pd.merge(Supplier, Risk_factor_data[[
    #                     'supplier', 'Risk_Factor']], on='supplier', how='left')


    Risk_factor_data = pd.DataFrame()

    Risk_factor_data['supplier_id'] = Supplier1['supplier_id'].unique().tolist()

    Risk_factor_data = pd.concat(
        [Risk_factor_data, Risk_factor_data_temp], axis=1)

    Supplier = pd.merge(Supplier, Risk_factor_data[[
                        'supplier_id', 'Risk_Factor']], on='supplier_id', how='left')


    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@




    # In[31]:

    supplier_customer_detail = pd.DataFrame()
    customer_detail = pd.DataFrame()

    # @@@@@@@@@@@@@@@@@@@@@@@

    # customer_detail = pd.merge(Customer_rating, contract_status, on=[
    #                            'supplier'], how='left')

    customer_detail = pd.merge(Customer_rating, contract_status, on=[
                               'supplier_id'], how='left')

    customer_detail.drop(columns=['supplier_y'],inplace=True)
    customer_detail.rename(columns={'supplier_x':'supplier'}, inplace=True)
    # @@@@@@@@@@@@@@@@@@@@@@@



    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@
    # supplier_customer_detail = pd.merge(customer_detail, open_payable, on=['supplier','display_name','status_short'], how='left')
    # supplier_customer_detail = pd.merge(supplier_customer_detail, credit_memo, on=['supplier','display_name','status_short'], how='left')
    # supplier_customer_detail = pd.merge(supplier_customer_detail, other_open_payables, on=['supplier','display_name','status_short'], how='left')
    # supplier_customer_detail = pd.merge(supplier_customer_detail, cash_balance, on=['supplier','display_name','status_short'], how='left')
    # supplier_customer_detail = pd.merge(supplier_customer_detail, unapplied_balance, on=['supplier','display_name','status_short'], how='left')


    supplier_customer_detail = pd.merge(customer_detail, open_payable, left_on='supplier_id', right_on='id',how='left')


    supplier_customer_detail.drop(columns=['status_short_y','display_name_y','id'], inplace=True)
    supplier_customer_detail.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    supplier_customer_detail = pd.merge(supplier_customer_detail, credit_memo, left_on='supplier_id', right_on='supplier_id',how='left')


    supplier_customer_detail.drop(columns=['status_short_y','display_name_y'], inplace=True)
    supplier_customer_detail.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    supplier_customer_detail = pd.merge(supplier_customer_detail, other_open_payables, left_on='supplier_id', right_on='client_id',how='left')


    supplier_customer_detail.drop(columns=['client_id'], inplace=True)


    supplier_customer_detail = pd.merge(supplier_customer_detail, cash_balance, left_on='supplier_id', right_on='id',how='left')


    supplier_customer_detail.drop(columns=['supplier_y', 'status_short_y','display_name_y','id'], inplace=True)
    supplier_customer_detail.rename(columns={'supplier_x':'supplier','status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)


    supplier_customer_detail = pd.merge(supplier_customer_detail, unapplied_balance, left_on='supplier_id', right_on='id',how='left')


    supplier_customer_detail.drop(columns=['status_short_y','display_name_y','id'], inplace=True)
    supplier_customer_detail.rename(columns={'status_short_x':'status_short','display_name_x':'display_name'}, inplace=True)

    # @@@@@@@@@@@@@@@@@@@@@@



    supplier_customer_detail[['open_payable', 'open_credit_memo',
                              'other_open_payables', 'cash_balance', 'unapplied_amount']] = supplier_customer_detail[['open_payable', 'open_credit_memo',
                                                                                                                      'other_open_payables', 'cash_balance', 'unapplied_amount']].fillna(0)
    supplier_customer_detail['wallet'] = supplier_customer_detail['unapplied_amount'] - \
        supplier_customer_detail['other_open_payables'] - \
        supplier_customer_detail['open_credit_memo']
    supplier_customer_detail['Supplier_Risk_Balance'] = (
        supplier_customer_detail['total_AR']-supplier_customer_detail['other_open_payables']-supplier_customer_detail['open_credit_memo'])+supplier_customer_detail['cash_balance']

    supplier_customer_detail['processing_date'] = my_date

    # # Snowflake database

    # In[32]:

    Supplier_Parameter_Weights = supplier_weight_df.rename(columns={'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice', 'Unpaid Invoice (%)': 'Unpaid_Invoice',
                                                                    'Open AR': 'Open_AR', 'Aging Balance': 'Aging_Balance', 'Outstanding Balance': 'Outstanding_Balance',
                                                                    'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio', 'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days',
                                                                    'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days', 'Payment Cycle': 'Payment_Cycle',
                                                                    'Delinquent Payment<10_days': 'Delinquent_Payment_less_10_days', 'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days',
                                                                    'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days', 'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days',
                                                                    'overdue chargebacks': 'overdue_chargebacks'})
    Supplier_Parameter_Weights['processing_date'] = my_date

    Predictor_ten = Aggregated_predict_table10.rename(columns={'client_name': 'supplier', 'customer_name': 'Customer',
                                                      'early_payment<10': 'early_payment_before_10', 'early_payment>10': 'early_payment_after_10', 'Predict_AR < 10days': 'Predict_AR_10days'})
    Predictor_ten['processing_date'] = my_date

    Predictor_thirty = Aggregated_predict_table30.rename(columns={'client_name': 'supplier', 'customer_name': 'Customer',
                                                         'early_payment<10': 'early_payment_before_10', 'early_payment>10': 'early_payment_after_10', 'Predict_AR < 30days': 'Predict_AR_30days'})
    Predictor_thirty['processing_date'] = my_date

    Customer_Score = Customer_rating.rename(columns={'Customer type': 'Customer_type', 'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice', 'Unpaid Invoice (%)': 'Unpaid_Invoice', 'Open AR': 'Open_AR', 'Aging Balance': 'Aging_Balance',
                                                     'Outstanding Balance': 'Outstanding_Balance', 'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio',
                                                     'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days', 'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days',
                                                     'Payment Cycle': 'Payment_Cycle', 'Delinquent Payment<10_days': 'Delinquent_Payment_less_10_days',
                                                     'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days',
                                                     'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days', 'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days',
                                                     '# Suppliers': 'Suppliers_freq', 'overdue chargebacks': 'overdue_chargebacks',
                                                     'Timeliness of chargeback': 'Timeliness_of_chargeback', 'Risk balance': 'Risk_balance', 'w/o open cb': 'w_o_open_cb',
                                                     'Predict_AR < 10days': 'Predict_AR_10days', 'Predict_AR < 30days': 'Predict_AR_30days'})
    Customer_Score['processing_date'] = my_date
    Customer_Score['is_flagged'] = 0

    supplier_customer_detail = supplier_customer_detail.rename(columns={'Customer type': 'Customer_type', 'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice', 'Unpaid Invoice (%)': 'Unpaid_Invoice', 'Open AR': 'Open_AR', 'Aging Balance': 'Aging_Balance',
                                                                        'Outstanding Balance': 'Outstanding_Balance', 'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio',
                                                                        'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days', 'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days',
                                                                        'Payment Cycle': 'Payment_Cycle', 'Delinquent Payment<10_days': 'Delinquent_Payment_less_10_days',
                                                                        'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days',
                                                                        'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days', 'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days',
                                                                        '# Suppliers': 'Suppliers_freq', 'overdue chargebacks': 'overdue_chargebacks',
                                                                        'Timeliness of chargeback': 'Timeliness_of_chargeback', 'Risk balance': 'Customer_Risk_balance', 'w/o open cb': 'w_o_open_cb',
                                                                        'Predict_AR < 10days': 'Predict_AR_10days', 'Predict_AR < 30days': 'Predict_AR_30days'})

    # @@@@@@@@@@@@@@
    #    Customer_Parameter_Weights = weight_df.rename(columns={'customer_type': 'Customer_type', 'customer_name': 'Customer',
    #                                                           'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice',
    #                                                           'Unpaid Invoice (%)': 'Unpaid_Invoice', 'Open AR': 'Open_AR',
    #                                                           'Aging Balance': 'Aging_Balance', 'Outstanding Balance': 'Outstanding_Balance',
    #                                                           'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio',
    #                                                           'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days',
    #                                                           'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days',
    #                                                           'Payment Cycle': 'Payment_Cycle', 'Delinquent Payment<10_days': 'Delinquent_Payment_less_10_days',
    #                                                           'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days',
    #                                                           'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days',
    #                                                           'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days',
    #                                                           'overdue chargebacks': 'overdue_chargebacks'})

    Customer_Parameter_Weights = weight_df.rename(columns={'customer_type': 'Customer_type', 'customer_id': 'customer_id',
                                                           'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice',
                                                           'Unpaid Invoice (%)': 'Unpaid_Invoice', 'Open AR': 'Open_AR',
                                                           'Aging Balance': 'Aging_Balance', 'Outstanding Balance': 'Outstanding_Balance',
                                                           'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio',
                                                           'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days',
                                                           'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days',
                                                           'Payment Cycle': 'Payment_Cycle', 'Delinquent Payment<10_days': 'Delinquent_Payment_less_10_days',
                                                           'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days',
                                                           'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days',
                                                           'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days',
                                                           'overdue chargebacks': 'overdue_chargebacks'})

    # @@@@@@@@@@@@@@

    Customer_Parameter_Weights['processing_date'] = my_date

    Supplier_Score = Supplier.rename(columns={'Open Invoices': 'Total_Invoice', 'Open AR': 'Open_AR',
                                     'Predict_AR < 10days': 'Predict_AR_10days', 'Predict_AR < 30days': 'Predict_AR_30days'})
    Supplier_Score['processing_date'] = my_date
    Supplier_Score['is_flagged'] = 0

    Predictor_ten.to_csv(
        '/home/ubuntu/Result/Predictor_ten.csv', index=False)
    Predictor_thirty.to_csv(
        '/home/ubuntu/Result/Predictor_thirty.csv', index=False)
    Customer_Score.to_csv(
        '/home/ubuntu/Result/Customer_Score.csv', index=False)
    Customer_Parameter_Weights.to_csv(
        '/home/ubuntu/Result/Customer_Parameter_Weights.csv', index=False)
    Supplier_Score.to_csv(
        '/home/ubuntu/Result/Supplier_Score.csv', index=False)
    Supplier_Parameter_Weights.to_csv(
        '/home/ubuntu/Result/Supplier_Parameter_Weights.csv', index=False)



    # # Aggregated Supplier Score

    # In[33]:

    #    def max_score(level):
    #        if level == 1:
    #            return 399
    #        elif level == 2:
    #            return 499
    #        elif level == 3:
    #            return 699
    #        elif level == 4:
    #            return 800

    #    def min_score(level):
    #        if level == 1:
    #            return 0
    #        elif level == 2:
    #            return 400
    #        elif level == 3:
    #            return 500
    #        elif level == 4:
    #            return 700

    def max_score(level):
        if level == 1:
            return 250
        elif level == 2:
            return 400
        elif level == 3:
            return 800

    def min_score(level):
        if level == 1:
            return 0
        elif level == 2:
            return 251
        elif level == 3:
            return 401            

    def weighted_average_dbt(group):
        return np.round((group['Total_Invoice'] * group['DBT']).sum() / group['Total_Invoice'].sum())

    def weighted_average_pc(group):
        return np.round((group['Total_Invoice'] * group['Payment_Cycle']).sum() / group['Total_Invoice'].sum())

    def update_score(row, Agg_customer_score_norm_temp1, international_cust_list):
        if row['customer_id'] in international_cust_list:
            match_row = Agg_customer_score_norm_temp1[Agg_customer_score_norm_temp1['customer_id']
                                                      == row['customer_id']]
            return match_row['Score1'].values[0] if len(match_row) > 0 else row['Score']
        else:
            return row['Score']

    # In[34]:
    Supplier_Score['Level'] = Supplier_Score['Level'].astype(int)
    customer_counts = Supplier_Score.groupby(
        ['supplier_id', 'Level']).size().reset_index(name='count')
    max_levels = customer_counts.groupby('supplier_id')['count'].idxmax()
    max_counts = customer_counts.loc[max_levels]

    supplier_normlaised_df = pd.DataFrame()
    Agg_supplier_score = pd.DataFrame()
    Agg_supplier_score_temp = pd.DataFrame()
    Agg_supplier_score_norm_temp1 = pd.DataFrame()

    Agg_supplier_score_temp = Supplier_Score[['supplier_id', 'supplier', 'display_name','status_short', 'Total_Invoice',
                                              'Open_AR', 'unapplied_amount', 'open_payable', 'open_credit_memo',
                                              'other_open_payables', 'cash_balance', 'wallet', 'Risk_Balance',
                                              'Predict_AR_10days', 'Predict_AR_30days', 'customer_score',
                                              'total_AR', 'Outstanding Balance', 'Score',
                                              'Risk_Factor', 'processing_date']]

    Agg_supplier_score = Agg_supplier_score_temp.groupby('supplier_id',as_index=False).agg({
        'Total_Invoice':'sum',
        'Open_AR':'sum',
        'unapplied_amount':'first',
        'open_payable':'first',
        'open_credit_memo':'first',
        'other_open_payables':'first',
        'cash_balance':'first',
        'wallet':'first',
        'Predict_AR_10days':'sum',
        'Predict_AR_30days':'sum',
        'customer_score':'mean',
        'total_AR':'sum',
        'Outstanding Balance':'sum',
        'Risk_Factor':'first',
        'supplier':'first',
        'display_name':'first',
        'status_short':'first',
        'processing_date':'first'})

    # Changed total_AR to Open_AR wrt feedback 12
    Agg_supplier_score['Risk_Balance'] = (Agg_supplier_score['Open_AR']-Agg_supplier_score['other_open_payables'] -
                                          Agg_supplier_score['open_credit_memo'])+Agg_supplier_score['cash_balance']
    Agg_supplier_score['Score'] = 0
    Agg_supplier_score['Level'] = 0

    Agg_supplier_score_metric = Agg_supplier_score[['supplier_id', 'Total_Invoice',
                                                    'total_AR', 'unapplied_amount', 'open_payable', 'open_credit_memo',
                                                    'other_open_payables', 'cash_balance', 'wallet', 'Risk_Balance', 'customer_score']]

    #    for i in range(1, 5):
    for i in range(1, 4):
        supplier_detail_list = max_counts[max_counts['Level']
                                          == i]['supplier_id'].to_list()
        if len(supplier_detail_list) != 0:
            Agg_supplier_score_norm_temp1 = Agg_supplier_score_metric[Agg_supplier_score_metric['supplier_id'].isin(
                supplier_detail_list)]
            Agg_supplier_score_norm_temp = Agg_supplier_score_norm_temp1.drop(columns=[
                                                                              'supplier_id'])
            Agg_supplier_score_norm = (
                Agg_supplier_score_norm_temp - Agg_supplier_score_norm_temp.mean()) / (Agg_supplier_score_norm_temp.std())
            Agg_supplier_score_norm.fillna(0, inplace=True)

    # -----------------------------------------------------------------------------------------------------

            min_max_constant = 0.01
            for row in Agg_supplier_score_norm.columns:
                for idx in range(len(Agg_supplier_score_norm)):
                    if Agg_supplier_score_norm_temp1[row].values[idx]== 0.0:
                        Agg_supplier_score_norm[row].values[idx] = 0

            Agg_supplier_score_norm_max_min = ((Agg_supplier_score_norm - Agg_supplier_score_norm.min()) + min_max_constant) / ((Agg_supplier_score_norm.max() - Agg_supplier_score_norm.min()) + min_max_constant)
            Agg_supplier_score_norm_max_min.fillna(0, inplace=True)

            Agg_supplier_score_norm = Agg_supplier_score_norm_max_min

            for row in Agg_supplier_score_norm.columns:
                for idx in range(len(Agg_supplier_score_norm)):
                    if Agg_supplier_score_norm_temp1[row].values[idx]== 0.0:
                        Agg_supplier_score_norm[row].values[idx] = 0

    # -----------------------------------------------------------------------------------------------------            


            Agg_supplier_score_norm['supplier_id'] = supplier_detail_list
            Agg_supplier_score_norm1 = Agg_supplier_score_norm[Agg_supplier_score_norm['supplier_id'].isin(
                supplier_detail_list)].drop(columns=['supplier_id'])
            
            # @@@@@@@@@@@@@
    #         old_weight = (supplier_weight_df.drop(
    #             columns='supplier')).iloc[0].to_dict()
            
            old_weight = (supplier_weight_df.drop(
                columns='supplier_id')).iloc[0].to_dict()
        
            # @@@@@@@@@@@@@
            
            new_weight = {'Total_Invoice': old_weight['Open Invoices'],
                          'total_AR': old_weight['total_AR'],
                          'unapplied_amount': old_weight['unapplied_amount'],
                          'open_payable': old_weight['open_payable'],
                          'open_credit_memo': old_weight['open_credit_memo'],
                          'other_open_payables': old_weight['other_open_payables'],
                          'cash_balance': old_weight['cash_balance'],
                          'wallet': old_weight['wallet'],
                          'Risk_Balance': old_weight['Risk_Balance'],
                          'customer_score': old_weight['customer_score']
                          }
            weighted_scores = Agg_supplier_score_norm1.mul(new_weight, axis=1)
            Agg_supplier_score_norm_temp1['Score'] = round(
                ((weighted_scores.sum(axis=1)) + 3) * 133.3333)
            Agg_supplier_score_norm_temp1['Score'] = Agg_supplier_score_norm_temp1['Score'].apply(
                lambda x: 0 if x < 0 else x)
            Agg_supplier_score_norm_temp1['Score'] = Agg_supplier_score_norm_temp1['Score'].apply(
                lambda x: 800 if x > 800 else x)
            Agg_supplier_score.loc[(Agg_supplier_score['supplier_id'].isin(
                supplier_detail_list)), 'Score'] = Agg_supplier_score_norm_temp1['Score']

            supplier_normlaised_df = pd.concat(
                [supplier_normlaised_df, Agg_supplier_score_norm], axis=0)

    #    Agg_supplier_score['Level'] = pd.cut(Agg_supplier_score['Score'],
    #                                         bins=[-1, 399, 499, 699, 800],
    #                                         labels=[1, 2, 3, 4])

    Agg_supplier_score['Level'] = pd.cut(Agg_supplier_score['Score'],
                               bins=[-1, 250,400,800],
                               labels=[1, 2, 3])


    Agg_supplier_score['is_flagged'] = 0
    Agg_supplier_score['previous_Score'] = -1
    Agg_supplier_score['previous_level'] = -1




    # In[35]:

    updated_levels = pd.read_sql_query(
        'SELECT * FROM SUPPLIER_UPDATE_LEVEL', cnn)

    sorted_df = updated_levels.sort_values('UPDATED_AT', ascending=False)
    updated_levels = sorted_df.drop_duplicates('SUPPLIER_ID', keep='first')
    updated_levels['SUPPLIER_ID'] = updated_levels['SUPPLIER_ID'].astype(float)
    updated_levels['NEW_LEVEL'] = updated_levels['NEW_LEVEL'].astype(int)

    supplier_std = Agg_supplier_score['Score'].std()
    # c = random.random()
    c = c1_value

    print("Suppliers for level update :: ",len(updated_levels['SUPPLIER_ID'].unique()))
    print(updated_levels['SUPPLIER_ID'].unique().tolist())




    if not updated_levels.empty:
        supplier_list_update_levels = updated_levels['SUPPLIER_ID'].unique(
        ).tolist()
        for i in supplier_list_update_levels:
            if i in Agg_supplier_score['supplier_id'].unique():
                print('Updating supplier ', i)
                x = updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0] - \
                    Agg_supplier_score[Agg_supplier_score['supplier_id']
                                       == i]['Level'].iloc[0]

                if x < 0:

                    mu = (Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Score'].iloc[0] - max_score(
                        updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0]))/(supplier_std)
                    x = -1
                else:
                    mu = (min_score(updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0]) -
                          Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Score'].iloc[0])/(supplier_std)
                    x = 1

                reason_variable = updated_levels[updated_levels['SUPPLIER_ID']
                                                 == i]['REASON'].iloc[0]

                Agg_supplier_score.loc[Agg_supplier_score['supplier_id'] == i,
                                       'previous_level'] = Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Level'].iloc[0]
                Agg_supplier_score.loc[Agg_supplier_score['supplier_id'] == i,
                                       'Level'] = updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0]

                Agg_supplier_score.loc[Agg_supplier_score['supplier_id'] == i,
                                       'previous_Score'] = Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Score'].iloc[0]
                Agg_supplier_score.loc[Agg_supplier_score['supplier_id'] == i, 'Score'] = round(
                    Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Score'].iloc[0] + ((mu+c)*supplier_std * x))
                
                supplier_name = Agg_supplier_score[Agg_supplier_score['supplier_id']
                                                   == i]['supplier'].iloc[0]
                
                
                # @@@@@@@@@@@@@@@
    #             a = supplier_weight_df[supplier_weight_df['supplier']
    #                                    == supplier_name]

                a = supplier_weight_df[supplier_weight_df['supplier_id']
                                       == i]
                
                # @@@@@@@@@@@@@@@
                
                
                mul_factor_supplier = Agg_supplier_score[Agg_supplier_score['supplier_id'] == i]['Score'].iloc[0] / \
                    Agg_supplier_score[Agg_supplier_score['supplier_id']
                                       == i]['previous_Score'].iloc[0]

                # @@@@@@@@@@@@@@@
    #             a.drop(columns='supplier', inplace=True)
                a.drop(columns='supplier_id', inplace=True)

                # @@@@@@@@@@@@@@@
                
                
                a = a.rename(columns={'Open Invoices': 'Total_Invoice'})
                if reason_variable == "If the company has a history of overdue/delinquency within the PSI, they were able to plausibly justify the overdue (examples: pending issues with the supplier, delay in the delivery of products after issuing the invoice, invoices issued with errors)":
                    a['external_factor'] = pd.read_sql_query(f"""SELECT WEIGHT FROM SUPPLIER_REASON 
                                                                WHERE SUPPLIER_ID={i} AND REASON LIKE '{reason_variable}%'
                                                        """, cnn).iloc[0][0]
                    a[['Open AR', 'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days']] = np.multiply(
                        a[['Open AR', 'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days']], mul_factor_supplier)
                    a['external_factor'] = np.multiply(
                        a['external_factor'], mul_factor_supplier) + 0.0001
                else:
                    a['external_factor'] = pd.read_sql_query(f"""SELECT WEIGHT FROM SUPPLIER_REASON 
                                                                WHERE SUPPLIER_ID={i} AND REASON LIKE '{reason_variable}%'
                                                        """, cnn).iloc[0][0]
                    a['external_factor'] = np.multiply(
                        a['external_factor'], mul_factor_supplier)+0.0001
                # print(a.values[0])
                if np.sum(a.values[0]) != 0:
                    a = a/np.sum(a.values[0])
                # print("sum: ", np.sum(a.values[0]))
                # print(a)
                a['external_factor'].fillna(0.1, inplace=True)
                sql = "UPDATE SUPPLIER_REASON SET WEIGHT = %s WHERE SUPPLIER_ID = %s AND REASON = %s"
                cs = cnn.cursor()
                cs.execute(
                    sql, (float(a['external_factor']), int(i), str(reason_variable)))

                a = a.to_dict(orient='records')[0]

                b = supplier_normlaised_df[supplier_normlaised_df['supplier_id'] == i]
                b.drop(columns='supplier_id', inplace=True)
                b['external_factor'] = 1

                updated_weighted_scores = b.mul(a, axis=1)
                max_score1 = max_score(
                    updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0])
                min_score1 = min_score(
                    updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0])

                # need to review wrt to new changes
                updated_scores = round((((updated_weighted_scores.sum(
                    axis=1).iloc[0]+10)/20) * (max_score1-min_score1))+min_score1)


                Agg_supplier_score.loc[Agg_supplier_score['supplier_id']
                                       == i, 'Score'] = updated_scores
            else:
                print("Supplier ID not in Agg_supplier_score ", i)





    # # Aggregate Customer Score

    # In[36]:
    Customer_Score['Level'] = Customer_Score['Level'].astype(int)
    customer_counts = Customer_Score.groupby(
        ['customer_id', 'Level']).size().reset_index(name='count')
    max_levels = customer_counts.groupby('customer_id')['count'].idxmax()
    max_counts = customer_counts.loc[max_levels]

    customer_normlaised_df = pd.DataFrame()
    Agg_customer_score = pd.DataFrame()
    Agg_customer_score_temp = pd.DataFrame()
    Agg_customer_score_norm_temp1 = pd.DataFrame()

    Agg_customer_score_temp = Customer_Score[['ndg_id', 'ndg_name', 'customer_id', 'Customer',
                                              'Customer_type_id', 'Customer_type', 'invoice_freq',
                                              'Total_Invoice', 'Paid_Invoice', 'Unpaid_Invoice', 'early_payment_perc',
                                              'early_payment_amount', 'early_payment_days', 'Open_AR',
                                              'Aging_Balance', 'Outstanding_Balance', 'Payment_Timeliness_Ratio',
                                              'Late_Payment_Ratio_less_10_days', 'Late_Payment_Ratio_more_10_days',
                                              'DBT', 'Payment_Cycle', 'Delinquent_Payment_less_10_days',
                                              'Delinquent_Overdueday_less_10_days', 'Delinquent_Payment_more_10_days',
                                              'Delinquent_Overdueday_more_10_days', 'Payment_predictor',
                                              'Suppliers_freq', 'chargeback_freq', 'chargeback_vol_amt',
                                              'chargeback_vol_paid', 'chargeback_vol_unpaid',
                                              'supplier_wo_overdue_cb', 'supplier_w_overdue_cb',
                                              'overdue_chargebacks', 'early_chargeback', 'Timeliness_of_chargeback',
                                              'Risk_balance', 'w_o_open_cb', 'total_AR', 'payment1_percent',
                                              'payment2_percent', 'open_chargeback_freq', 'open_chargeback_vol_amt',
                                              'Score', 'Predict_AR_10days', 'Predict_AR_30days', 'num_supplier_w_open_invoice', 'payment2_percent_wo_cb',
                                              'processing_date']]

    Agg_customer_score = Agg_customer_score_temp.groupby('customer_id', as_index=False).agg(
        {'invoice_freq': 'sum',
         'Total_Invoice': 'sum',
         'early_payment_perc': 'mean',
         'early_payment_amount': 'sum',
         'early_payment_days': 'mean',
         'Open_AR': 'sum',
         'Aging_Balance': 'mean',
         'Outstanding_Balance': 'sum',
         'Payment_Timeliness_Ratio': 'sum',
         'Late_Payment_Ratio_less_10_days': 'sum',
         'Late_Payment_Ratio_more_10_days': 'sum',
         'Delinquent_Payment_less_10_days': 'sum',
         'Delinquent_Overdueday_less_10_days': 'mean',
         'Delinquent_Payment_more_10_days': 'sum',
         'Delinquent_Overdueday_more_10_days': 'mean',
         'Payment_predictor': 'mean',
         'Suppliers_freq': 'sum',
         'chargeback_freq': 'sum',
         'chargeback_vol_amt': 'sum',
         'chargeback_vol_paid': 'sum',
         'chargeback_vol_unpaid': 'sum',
         'supplier_wo_overdue_cb': 'sum',
         'supplier_w_overdue_cb': 'sum',
         'overdue_chargebacks': 'sum',
         'early_chargeback': 'sum',
         'Timeliness_of_chargeback': 'mean',
         'Risk_balance': 'sum',
         'w_o_open_cb': 'mean',
         'total_AR': 'sum',
         'payment1_percent': 'mean',
         'payment2_percent': 'mean',
         'open_chargeback_freq': 'sum',
         'open_chargeback_vol_amt': 'sum',
         'Predict_AR_10days': 'sum',
         'Predict_AR_30days': 'sum',
         'num_supplier_w_open_invoice': 'sum',
                                        'payment2_percent_wo_cb': 'mean',
                                        'ndg_id': 'first',
                                        'ndg_name': 'first',
         'Customer': 'first',
         'Customer_type_id': 'first',
         'Customer_type': 'first',
                                        'processing_date': 'first'

         })
    Agg_customer_score['Score'] = 0
    Agg_customer_score['Level'] = 0

    sum_percent = Agg_customer_score[[
        'Payment_Timeliness_Ratio', 'Late_Payment_Ratio_less_10_days', 'Late_Payment_Ratio_more_10_days']].sum(axis=1)
    sum_percent[sum_percent == 0] = 1

    Agg_customer_score['Payment_Timeliness_Ratio'] = (
        Agg_customer_score['Payment_Timeliness_Ratio']/sum_percent)*100
    Agg_customer_score['Late_Payment_Ratio_less_10_days'] = (
        Agg_customer_score['Late_Payment_Ratio_less_10_days']/sum_percent)*100
    Agg_customer_score['Late_Payment_Ratio_more_10_days'] = (
        Agg_customer_score['Late_Payment_Ratio_more_10_days']/sum_percent)*100

    Agg_customer_score['DBT'] = Agg_customer_score_temp.groupby('customer_id').apply(
        weighted_average_dbt).reset_index(name='weighted_average_dbt')['weighted_average_dbt']
    Agg_customer_score['Payment_Cycle'] = Agg_customer_score_temp.groupby('customer_id').apply(
        weighted_average_pc).reset_index(name='weighted_average_pc')['weighted_average_pc']

    Agg_customer_score['Unpaid_Invoice'] = (
        Agg_customer_score['total_AR']/Agg_customer_score['Total_Invoice'])*100
    Agg_customer_score['Paid_Invoice'] = 100 - \
        Agg_customer_score['Unpaid_Invoice']

    Agg_customer_score['DBT'] = Agg_customer_score['DBT'].fillna(0)
    Agg_customer_score['Payment_Cycle'] = Agg_customer_score['Payment_Cycle'].fillna(
        0)
    Agg_customer_score['Paid_Invoice'] = Agg_customer_score['Paid_Invoice'].fillna(
        0)
    Agg_customer_score['Unpaid_Invoice'] = Agg_customer_score['Unpaid_Invoice'].fillna(
        0)

    international_cust_list = Agg_customer_score[Agg_customer_score['Customer_type_id'] == 4]['customer_id'].to_list(
    )

    Agg_customer_score_metric = Agg_customer_score[['customer_id', 'Total_Invoice', 'Paid_Invoice',
                                                    'Open_AR', 'Aging_Balance', 'Outstanding_Balance',
                                                    'Payment_Timeliness_Ratio', 'Late_Payment_Ratio_less_10_days',
                                                    'Late_Payment_Ratio_more_10_days', 'DBT', 'Payment_Cycle',
                                                    'Delinquent_Overdueday_less_10_days',
                                                    'Delinquent_Payment_more_10_days', 'Delinquent_Overdueday_more_10_days',
                                                    'chargeback_vol_amt', 'overdue_chargebacks']]
    original_data_types = weight_df.dtypes.to_dict()

    weight_df['customer_id'] = weight_df['customer_id'].astype(int)

    Agg_weight_df = weight_df.sort_values('customer_id').drop(columns=['early_chargeback', 'Delinquent Payment<10_days', 'Unpaid Invoice (%)',
                                                                       'chargeback_freq', 'customer_name', 'customer_id', 'ndg_id', 'ndg_name', 'Customer_type_id', 'customer_type'])

    weight_df['customer_id'] = weight_df['customer_id'].astype(
        original_data_types['customer_id'])

    # testing

    # In[38]:

    Agg_customer_score_metric['Payment_Cycle'] = Agg_customer_score_metric['Payment_Cycle']-(
        Agg_customer_score_metric['Payment_Cycle']-Agg_customer_score_metric['DBT'])
    Agg_weight_df = Agg_weight_df.rename(columns={'Open Invoices': 'Total_Invoice', 'Paid Invoice (%)': 'Paid_Invoice',
                                                  'Open AR': 'Open_AR', 'Aging Balance': 'Aging_Balance', 'Outstanding Balance': 'Outstanding_Balance',
                                                  'Payment Timeliness Ratio (%)': 'Payment_Timeliness_Ratio', 'Late Payment Ratio<10_days': 'Late_Payment_Ratio_less_10_days',
                                                  'Late Payment Ratio>10_days': 'Late_Payment_Ratio_more_10_days', 'Payment Cycle': 'Payment_Cycle',
                                                  'Delinquent Overdueday<10_days': 'Delinquent_Overdueday_less_10_days', 'Delinquent Payment>10_days': 'Delinquent_Payment_more_10_days',
                                                  'Delinquent Overdueday>10_days': 'Delinquent_Overdueday_more_10_days', 'overdue chargebacks': 'overdue_chargebacks'})

    Agg_weight_df.loc[Agg_weight_df['Payment_Cycle'] > 0,
                      'Payment_Cycle'] = -Agg_weight_df['Payment_Cycle']

    Agg_customer_score_norm_temp = Agg_customer_score_metric.drop(columns=[
                                                                  'customer_id'])
    Agg_customer_score_norm = (Agg_customer_score_norm_temp - Agg_customer_score_norm_temp.min())/(
        Agg_customer_score_norm_temp.max() - Agg_customer_score_norm_temp.min())

    customer_detail_list = max_counts['customer_id'].to_list()

    Agg_customer_score_norm.fillna(0, inplace=True)
    Agg_customer_score_norm['customer_id'] = customer_detail_list

    customer_normlaised_df = Agg_customer_score_norm

    weighted_scores = (Agg_customer_score_norm)*(Agg_weight_df)
    weighted_scores = weighted_scores.sum(axis=1)

    Agg_customer_score['Score'] = np.round(
        ((weighted_scores-weighted_scores.min())/(weighted_scores.max()-weighted_scores.min()))*800)


    # Agg_customer_score['Score1'] = np.round(
    #     ((weighted_scores-weighted_scores.min())/(weighted_scores.max()-weighted_scores.min()))*399)

    Agg_customer_score['Score1'] = np.round(
        ((weighted_scores-weighted_scores.min())/(weighted_scores.max()-weighted_scores.min()))*240)

    Agg_customer_score['Score'] = Agg_customer_score['Score'].apply(
        lambda x: 10 if x < 0 else x)
    Agg_customer_score['Score'] = Agg_customer_score['Score'].apply(
        lambda x: 780 if x > 800 else x)

    Agg_customer_score['Score1'] = Agg_customer_score['Score1'].apply(
        lambda x: 10 if x < 0 else x)
    Agg_customer_score['Score1'] = Agg_customer_score['Score1'].apply(
        lambda x: 780 if x > 800 else x)

    Agg_customer_score.loc[Agg_customer_score['Customer_type_id']
                           == 4, 'Score'] = Agg_customer_score['Score1']

    Agg_customer_score = Agg_customer_score.drop(columns=['Score1'])

    #    Agg_customer_score['Level'] = pd.cut(Agg_customer_score['Score'],
    #                                         bins=[-1, 399, 499, 699, 800],
    #                                         labels=[1, 2, 3, 4])

    Agg_customer_score['Level'] = pd.cut(Agg_customer_score['Score'],
                               bins=[-1, 250,400,800],
                               labels=[1, 2, 3])


    Agg_customer_score['is_flagged'] = 0

    Agg_customer_score['previous_Score'] = -1
    Agg_customer_score['previous_level'] = -1





    # In[42]:

    updated_levels = pd.read_sql_query(
        'SELECT * FROM CUSTOMER_UPDATE_LEVEL', cnn)

    sorted_df = updated_levels.sort_values('UPDATED_AT', ascending=False)
    updated_levels = sorted_df.drop_duplicates('CUSTOMER_ID', keep='first')
    updated_levels['CUSTOMER_ID'] = updated_levels['CUSTOMER_ID'].astype(float)
    updated_levels['NEW_LEVEL'] = updated_levels['NEW_LEVEL'].astype(int)

    customer_std = Agg_customer_score['Score'].std()
    # c = random.random()
    c = c2_value

    # changed 399 with the uppler limit on level 1 which is 250
    #    mu1 = (Agg_customer_score['Score'] - 399)/(customer_std)
    mu1 = (Agg_customer_score['Score'] - 250)/(customer_std)

    Agg_customer_score.loc[Agg_customer_score['Customer_type_id'] == 4, 'Score'] = round(
        Agg_customer_score['Score'][0] + ((mu1+c)*customer_std))

    Agg_customer_score['Level'] = pd.cut(Agg_customer_score['Score'],
                                         bins=[-1, 250,400,800],
                                         labels=[1, 2, 3])

    print("Customers for level update :: ",len(updated_levels['CUSTOMER_ID'].unique()))
    print(updated_levels['CUSTOMER_ID'].unique().tolist())



    if not updated_levels.empty:
        customer_list_update_levels = updated_levels['CUSTOMER_ID'].unique(
        ).tolist()
        for i in customer_list_update_levels:
            if i in Agg_customer_score['customer_id'].unique():
                print('Updating customer ', i)
                x = updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0] - \
                    Agg_customer_score[Agg_customer_score['customer_id']
                                       == i]['Level'].iloc[0]

                if x < 0:
                    mu = (Agg_customer_score[Agg_customer_score['customer_id'] == i]['Score'].iloc[0] - max_score(
                        updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0]))/(customer_std)
                    x = -1
                else:
                    mu = (min_score(updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0]) -
                          Agg_customer_score[Agg_customer_score['customer_id'] == i]['Score'].iloc[0])/(customer_std)
                    x = 1

                reason_variable = updated_levels[updated_levels['CUSTOMER_ID']
                                                 == i]['REASON'].iloc[0]

                Agg_customer_score.loc[Agg_customer_score['customer_id'] == i,
                                       'previous_level'] = Agg_customer_score[Agg_customer_score['customer_id'] == i]['Level'].iloc[0]
                Agg_customer_score.loc[Agg_customer_score['customer_id'] == i,
                                       'Level'] = updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0]

                Agg_customer_score.loc[Agg_customer_score['customer_id'] == i,
                                       'previous_Score'] = Agg_customer_score[Agg_customer_score['customer_id'] == i]['Score'].iloc[0]
                Agg_customer_score.loc[Agg_customer_score['customer_id'] == i, 'Score'] = round(
                    Agg_customer_score[Agg_customer_score['customer_id'] == i]['Score'].iloc[0] + ((mu+c)*customer_std*x))

                customer_name = Agg_customer_score[Agg_customer_score['customer_id']
                                                   == i]['Customer'].iloc[0]
                
                # @@@@@@@@@@@@@@@

    #             a = weight_df[weight_df['customer_name'] == customer_name]

                a = weight_df[weight_df['customer_id'] == int(customer_id)]
        
                # @@@@@@@@@@@@@@@
                
                

                mul_factor_customer = Agg_customer_score[Agg_customer_score['customer_id'] == i]['Score'].iloc[0] / \
                    Agg_customer_score[Agg_customer_score['customer_id']
                                       == i]['previous_Score'].iloc[0]
                
                a.drop(columns=['customer_name', 'customer_id', 'ndg_id',
                       'ndg_name', 'Customer_type_id', 'customer_type'], inplace=True)

                a = a.rename(columns={'Open Invoices': 'Total_Invoice'})
                if reason_variable == "If the company has a history of overdue/delinquency within the PSI, they were able to plausibly justify the overdue (examples: pending issues with the supplier, delay in the delivery of products after issuing the invoice, invoices issued with errors)":
                    a['external_factor'] = pd.read_sql_query(f"""SELECT WEIGHT FROM CUSTOMER_REASON 
                                                                WHERE CUSTOMER_ID={i} AND REASON LIKE '{reason_variable}%'
                                                        """, cnn).iloc[0][0]
                    a[['Open AR', 'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days']] = np.multiply(
                        a[['Open AR', 'Delinquent Payment<10_days', 'Delinquent Overdueday<10_days', 'Delinquent Payment>10_days', 'Delinquent Overdueday>10_days']], mul_factor_supplier)
                    a['external_factor'] = np.multiply(
                        a['external_factor'], mul_factor_customer) + 0.0001
                # elif (reason_variable != "If the company has a history of overdue/delinquency within the PSI, they were able to plausibly justify the overdue (examples: pending issues with the supplier, delay in the delivery of products after issuing the invoice, invoices issued with errors)") | (reason_variable !='Conflict of interests') | (reason_variable !='High level of rejected chargebacks') & (reason_variable != 'Customer dependency (more concentrated or more diversified)'):
                else:
                    a['external_factor'] = pd.read_sql_query(f"""SELECT WEIGHT FROM CUSTOMER_REASON 
                                                                WHERE CUSTOMER_ID={i} AND REASON LIKE '{reason_variable}%'
                                                        """, cnn).iloc[0][0]
                    a['external_factor'] = np.multiply(
                        a['external_factor'], mul_factor_customer)+0.0001

                if np.sum(a.values[0]) != 0:
                    a = a/np.sum(a.values[0])

                sql = "UPDATE CUSTOMER_REASON SET WEIGHT = %s WHERE CUSTOMER_ID = %s AND REASON = %s"
                cs = cnn.cursor()
                cs.execute(
                    sql, (float(a['external_factor']), int(i), str(reason_variable)))

                # a = a.to_dict(orient='records')[0]

                b = customer_normlaised_df[customer_normlaised_df['customer_id'] == i]
                b.drop(columns='customer_id', inplace=True)
                b['external_factor'] = 1

                updated_weighted_scores = b.mul(a, axis=1)
                max_score1 = max_score(
                    updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0])
                min_score1 = min_score(
                    updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0])
                updated_scores = round((((updated_weighted_scores.sum(
                    axis=1).iloc[0]+10)/20) * (max_score1-min_score1))+min_score1)
                Agg_customer_score.loc[Agg_customer_score['customer_id']
                                       == i, 'Score'] = updated_scores
            else:
                print("Customer id not present in Agg_customer_score ", i)



    # In[43]:

    ndg_Score_temp = pd.DataFrame()
    ndg_Score = pd.DataFrame()
    ndg_counts = Customer_Score.groupby(
        ['ndg_id', 'Level']).size().reset_index(name='count')
    max_levels = ndg_counts.groupby('ndg_id')['count'].idxmax()
    max_counts = ndg_counts.loc[max_levels]

    ndg_Score_temp = Customer_Score[['ndg_id', 'ndg_name', 'invoice_freq',
                                     'Total_Invoice', 'Paid_Invoice', 'Unpaid_Invoice', 'early_payment_perc',
                                     'early_payment_amount', 'early_payment_days', 'Open_AR',
                                     'Aging_Balance', 'Outstanding_Balance', 'Payment_Timeliness_Ratio',
                                     'Late_Payment_Ratio_less_10_days', 'Late_Payment_Ratio_more_10_days',
                                     'DBT', 'Payment_Cycle', 'Delinquent_Payment_less_10_days',
                                     'Delinquent_Overdueday_less_10_days', 'Delinquent_Payment_more_10_days',
                                     'Delinquent_Overdueday_more_10_days', 'Payment_predictor',
                                     'Suppliers_freq', 'chargeback_freq', 'chargeback_vol_amt',
                                     'chargeback_vol_paid', 'chargeback_vol_unpaid',
                                     'supplier_wo_overdue_cb', 'supplier_w_overdue_cb',
                                     'overdue_chargebacks', 'early_chargeback', 'Timeliness_of_chargeback',
                                     'Risk_balance', 'w_o_open_cb', 'total_AR', 'payment1_percent',
                                     'payment2_percent', 'open_chargeback_freq', 'open_chargeback_vol_amt',
                                     'Score', 'Predict_AR_10days', 'Predict_AR_30days', 'num_supplier_w_open_invoice', 'payment2_percent_wo_cb',
                                     'processing_date']]

    ndg_Score = ndg_Score_temp.groupby(['ndg_id'], as_index=False).agg({'invoice_freq': 'sum',
                                                                        'Total_Invoice': 'sum',
                                                                       'early_payment_perc': 'mean',
                                                                        'early_payment_amount': 'sum',
                                                                        'early_payment_days': 'mean',
                                                                        'Open_AR': 'sum',
                                                                        'Aging_Balance': 'mean',
                                                                        'Outstanding_Balance': 'sum',
                                                                        'Payment_Timeliness_Ratio': 'sum',
                                                                        'Late_Payment_Ratio_less_10_days': 'sum',
                                                                        'Late_Payment_Ratio_more_10_days': 'sum',
                                                                        'Delinquent_Payment_less_10_days': 'sum',
                                                                        'Delinquent_Overdueday_less_10_days': 'mean',
                                                                        'Delinquent_Payment_more_10_days': 'sum',
                                                                        'Delinquent_Overdueday_more_10_days': 'mean',
                                                                        'Payment_predictor': 'mean',
                                                                        'Suppliers_freq': 'sum',
                                                                        'chargeback_freq': 'sum',
                                                                        'chargeback_vol_amt': 'sum',
                                                                        'chargeback_vol_paid': 'sum',
                                                                        'chargeback_vol_unpaid': 'sum',
                                                                        'supplier_wo_overdue_cb': 'sum',
                                                                        'supplier_w_overdue_cb': 'sum',
                                                                        'overdue_chargebacks': 'sum',
                                                                        'early_chargeback': 'sum',
                                                                        'Timeliness_of_chargeback': 'mean',
                                                                        'Risk_balance': 'sum',
                                                                        'w_o_open_cb': 'mean',
                                                                        'total_AR': 'sum',
                                                                        'payment1_percent': 'mean',
                                                                        'payment2_percent': 'mean',
                                                                        'open_chargeback_freq': 'sum',
                                                                        'open_chargeback_vol_amt': 'sum',
                                                                        'Predict_AR_10days': 'sum',
                                                                        'Predict_AR_30days': 'sum',
                                                                        'num_supplier_w_open_invoice': 'sum',
                                                                        'payment2_percent_wo_cb': 'mean',
                                                                        'ndg_name': 'first',
                                                                        'processing_date': 'first'

                                                                        })

    ndg_Score['Score'] = 0
    ndg_Score['Level'] = 0

    ndg_sum_percent = ndg_Score[['Payment_Timeliness_Ratio',
                                 'Late_Payment_Ratio_less_10_days', 'Late_Payment_Ratio_more_10_days']].sum(axis=1)
    ndg_sum_percent[ndg_sum_percent == 0] = 1

    ndg_Score['Payment_Timeliness_Ratio'] = (
        ndg_Score['Payment_Timeliness_Ratio']/ndg_sum_percent)*100
    ndg_Score['Late_Payment_Ratio_less_10_days'] = (
        ndg_Score['Late_Payment_Ratio_less_10_days']/ndg_sum_percent)*100
    ndg_Score['Late_Payment_Ratio_more_10_days'] = (
        ndg_Score['Late_Payment_Ratio_more_10_days']/ndg_sum_percent)*100

    ndg_Score['DBT'] = ndg_Score_temp.groupby('ndg_id').apply(
        weighted_average_dbt).reset_index(name='weighted_average_dbt')['weighted_average_dbt']
    ndg_Score['Payment_Cycle'] = ndg_Score_temp.groupby('ndg_id').apply(
        weighted_average_pc).reset_index(name='weighted_average_pc')['weighted_average_pc']

    ndg_Score['Unpaid_Invoice'] = (
        ndg_Score['total_AR']/ndg_Score['Total_Invoice'])*100
    ndg_Score['Paid_Invoice'] = 100-ndg_Score['Unpaid_Invoice']

    ndg_Score['DBT'] = ndg_Score['DBT'].fillna(0)
    ndg_Score['Payment_Cycle'] = ndg_Score['Payment_Cycle'].fillna(0)
    ndg_Score['Paid_Invoice'] = ndg_Score['Paid_Invoice'].fillna(0)
    ndg_Score['Unpaid_Invoice'] = ndg_Score['Unpaid_Invoice'].fillna(0)

    ndg_Score_metric = ndg_Score[['ndg_id', 'Total_Invoice', 'Paid_Invoice', 'Unpaid_Invoice',
                                  'Open_AR', 'Aging_Balance', 'Outstanding_Balance',
                                  'Payment_Timeliness_Ratio', 'Late_Payment_Ratio_less_10_days',
                                  'Late_Payment_Ratio_more_10_days', 'DBT', 'Payment_Cycle',
                                  'Delinquent_Payment_less_10_days', 'Delinquent_Overdueday_less_10_days',
                                  'Delinquent_Payment_more_10_days', 'Delinquent_Overdueday_more_10_days',
                                  'chargeback_freq', 'chargeback_vol_amt', 'overdue_chargebacks', 'early_chargeback', 'Risk_balance']]
    #    for i in range(1, 5):
    for i in range(1, 4):
        ndg_detail_list = max_counts[max_counts['Level']
                                     == i]['ndg_id'].to_list()
        if len(ndg_detail_list) != 0:
            ndg_Score_norm_temp1 = ndg_Score_metric[ndg_Score_metric['ndg_id'].isin(
                ndg_detail_list)]
            ndg_Score_norm_temp = ndg_Score_norm_temp1.drop(columns=['ndg_id'])

            corr_df1 = ndg_Score_norm_temp.drop(columns=['Risk_balance'])
            corr_df2 = ndg_Score_norm_temp['Risk_balance']

            if (ndg_Score_norm_temp['Risk_balance'].eq(0).all()) | (len(ndg_detail_list) == 1):

                # weights = {'Total_Invoice': 0.68,
                #            'Paid_Invoice': 0.31,
                #            'Unpaid_Invoice': -0.31,
                #            'Open_AR': -1,
                #            'Aging_Balance': -1,
                #            'Outstanding_Balance': 0,
                #            'Payment_Timeliness_Ratio': 0.21,
                #            'Late_Payment_Ratio_less_10_days': 0.028731973955494355,
                #            'Late_Payment_Ratio_more_10_days': -0.09327659717169373,
                #            'DBT': -0.01,
                #            'Payment_Cycle': -0.01,
                #            'Delinquent_Payment_less_10_days': 0.14,
                #            'Delinquent_Overdueday_less_10_days': 0.11,
                #            'Delinquent_Payment_more_10_days': 0.07,
                #            'Delinquent_Overdueday_more_10_days': -0.06,
                #            'chargeback_freq': -0.07,
                #            'chargeback_vol_amt': -0.07,
                #            'overdue chargebacks': -0.06,
                #            'early_chargeback': -0.07
                #            }

                # Changed Unpaid_Invoice to 0 :: Reason :: No mentioned in the list.
                # Changed Late_Payment_Ratio_less_10_days to 1 :: Reason :: ML decided whether it would be positive or negative on a per customer basis.
                # Changed chargeback_freq to 0 :: Reason :: No mentioned in the list.
                # Changed early_chargeback to 0 :: Reason :: No mentioned in the list.

                weights = {'Total_Invoice': 0.68,
                           'Paid_Invoice': 0.31,
                           'Unpaid_Invoice': 0,
                           'Open_AR': -1,
                           'Aging_Balance': -1,
                           'Outstanding_Balance': 0,
                           'Payment_Timeliness_Ratio': 0.21,
                           'Late_Payment_Ratio_less_10_days': 1,
                           'Late_Payment_Ratio_more_10_days': -0.09327659717169373,
                           'DBT': -0.01,
                           'Payment_Cycle': -0.01,
                           'Delinquent_Payment_less_10_days': 0.14,
                           'Delinquent_Overdueday_less_10_days': 0.11,
                           'Delinquent_Payment_more_10_days': -0.07,
                           'Delinquent_Overdueday_more_10_days': -0.06,
                           'chargeback_freq': 0,
                           'chargeback_vol_amt': -0.07,
                           'overdue_chargebacks': -0.06,
                           'early_chargeback': 0
                           }
            else:

                corr_matrix = corr_df1.corrwith(corr_df2)
                weights = {col: corr_matrix[col] for col in corr_matrix.index}

                for key, value in weights.items():
                    if math.isnan(value):
                        weights[key] = 0
                # key to change
                # keys_to_change_p = ['Paid_Invoice',
                #                     'Total_Invoice', 'Payment_Timeliness_Ratio']

                keys_to_change_p = ['Total_Invoice', 'Paid_Invoice', 'Payment_Timeliness_Ratio', 
                    'Delinquent_Payment_less_10_days', 'Delinquent_Overdueday_less_10_days']

                for key in keys_to_change_p:
                    if weights[key] < 0:
                        weights[key] *= -1

                # keys_to_change_n = ['Unpaid_Invoice',
                #                     'Aging_Balance', 'Outstanding_Balance', 'DBT']

                # keys_to_change_n = ['Unpaid_Invoice','Aging_Balance','Outstanding_Balance','DBT','Open_AR','chargeback_freq','chargeback_vol_amt','Late_Payment_Ratio_less_10_days','Late_Payment_Ratio_more_10_days', 'Payment_Cycle']

                keys_to_change_n = ['Open_AR', 'Aging_Balance', 
                    'Outstanding_Balance' ,'Late_Payment_Ratio_more_10_days', 'DBT', 
                    'Payment_Cycle', 'Delinquent_Payment_more_10_days', 
                    'Delinquent_Overdueday_more_10_days', 
                    'chargeback_vol_amt', 'overdue_chargebacks']

                for key in keys_to_change_n:
                    if weights[key] > 0:
                        weights[key] *= -1

            ndg_Score_norm = (corr_df1 - corr_df1.mean()) / (corr_df1.std())
            ndg_Score_norm.fillna(0, inplace=True)
            ndg_Score_norm['ndg_id'] = ndg_detail_list

            # keys_to_change_n1 = ['Paid_Invoice', 'Total_Invoice','Payment_Timeliness_Ratio','Unpaid_Invoice','Aging_Balance','Outstanding_Balance','DBT','Open_AR','chargeback_freq','chargeback_vol_amt','Late_Payment_Ratio_less_10_days','Late_Payment_Ratio_more_10_days', 'Payment_Cycle']
            keys_to_change_n1 = ['Total_Invoice', 'Paid_Invoice', 'Payment_Timeliness_Ratio', 
                                    'Delinquent_Payment_less_10_days', 'Delinquent_Overdueday_less_10_days',
                                'Open_AR', 'Aging_Balance', 
                                    'Outstanding_Balance' ,'Late_Payment_Ratio_more_10_days', 'DBT', 
                                    'Payment_Cycle', 'Delinquent_Payment_more_10_days', 
                                    'Delinquent_Overdueday_more_10_days', 
                                    'chargeback_vol_amt', 'overdue_chargebacks']

            for key1 in keys_to_change_n1:
                ndg_Score_norm[key1] = ndg_Score_norm[key1].apply(lambda x: -x if x < 0 else x)

            ndg_Score_norm1 = ndg_Score_norm[ndg_Score_norm['ndg_id'].isin(
                ndg_detail_list)].drop(columns=['ndg_id'])
            weighted_scores = ndg_Score_norm1.mul(weights, axis=1)
            ndg_Score_norm_temp1['Score'] = round(
                ((weighted_scores.sum(axis=1)) + 3) * 133.3333)
            ndg_Score_norm_temp1['Score'] = ndg_Score_norm_temp1['Score'].apply(
                lambda x: 0 if x < 0 else x)
            ndg_Score_norm_temp1['Score'] = ndg_Score_norm_temp1['Score'].apply(
                lambda x: 800 if x > 800 else x)
            ndg_Score.loc[(ndg_Score['ndg_id'].isin(ndg_detail_list)),
                          'Score'] = ndg_Score_norm_temp1['Score']

    #    ndg_Score['Level'] = pd.cut(ndg_Score['Score'],
    #                                bins=[-1, 399, 499, 699, 800],
    #                                labels=[1, 2, 3, 4])

    ndg_Score['Level'] = pd.cut(ndg_Score['Score'],
                               bins=[-1, 250,400,800],
                               labels=[1, 2, 3])

    ndg_Score['Customer_type'] = 'NDG'

    # In[44]:
    ndg_map = {}

    for entry in map_df.values:
        t_ndg_id = entry[2]
        t_cust_id = entry[0]

        if t_ndg_id not in ndg_map:
            ndg_map[t_ndg_id] = [t_cust_id]
        else:
            ndg_map[t_ndg_id].append(t_cust_id)

    ndg_map_only_one = {}
    for key, value in ndg_map.items():
        if len(value) == 1:
            ndg_map_only_one[key] = value

    for t_ndg_id, t_cust_id in ndg_map_only_one.items():
        t_cust_id = t_cust_id[0]

        try:
            ndg_Score.loc[ndg_Score['ndg_id'] == t_ndg_id,
                          'Score'] = Agg_customer_score[Agg_customer_score['customer_id'] == t_cust_id]['Score'].values[0]
        except:
            ndg_Score.loc[ndg_Score['ndg_id'] == t_ndg_id,
                          'Score'] = Agg_customer_score[Agg_customer_score['customer_id'] == int(t_cust_id)]['Score'].values[0]

    # ndg_Score.to_csv('updated_ndg_score.csv', header=True, index=False)


    # NDG Post-processing

    df_1 = pd.merge(map_df, ndg_Score[['ndg_id', 'Score']], on=['ndg_id'], how='left')
    df_1.rename(columns={'Score':'old_ndg_score'},inplace=True)
    # df_1

    df_2 = pd.merge(df_1, Agg_customer_score[['customer_id', 'Score']], on=['customer_id'], how='left')
    df_2.rename(columns={'Score':'cust_score'}, inplace=True)
    #df_2

    v = df_2[['ndg_id','old_ndg_score','cust_score']]
    test3 = v.groupby(['ndg_id'],as_index=False).agg(cust_min_score=pd.NamedAgg(column="cust_score", aggfunc="min"),
                                                    cust_max_score=pd.NamedAgg(column="cust_score", aggfunc="max"),
                                                    old_ndg_score=pd.NamedAgg(column="old_ndg_score", aggfunc="mean"),
                                                    cust_mean_score=pd.NamedAgg(column="cust_score", aggfunc="mean")
                                                    )

    test3['normalised_ndg_score'] = test3['cust_mean_score']-test3['old_ndg_score']/(test3['cust_max_score'] - test3['cust_min_score'])
    test3['normalised_ndg_score'] = np.where(np.isinf(test3['normalised_ndg_score']), test3['old_ndg_score'], test3['normalised_ndg_score'])

    # Set 'new_ndg_score' to 0 where 'ndg_id' is equal to -1
    test3.loc[test3['ndg_id'] == -1, 'normalised_ndg_score'] = 0

    test3['normalised_ndg_score'].replace([np.inf, -np.inf, np.nan], 0, inplace=True)

    test3['normalised_ndg_score'] = test3['normalised_ndg_score'].astype(int)

    # Replacing normalised ndg score with old score where diff_customer min max is < 10 and 
    # diff_cust_mean with normalised ndg score is > 30

    test3['diff_cust_max_min'] = abs(test3['cust_min_score'] - test3['cust_max_score'])
    test3['diff_mean_normalised']=abs(test3['cust_mean_score'] - test3['normalised_ndg_score'])

    test3.loc[(test3['diff_cust_max_min'] < 10) & 
              (test3['diff_mean_normalised'] > 30) & 
              (test3['ndg_id']!= -1), 'normalised_ndg_score'] = test3['old_ndg_score']

    ndg_Score = pd.merge(ndg_Score, test3[['ndg_id', 'normalised_ndg_score']], on=['ndg_id'], how='left')
    ndg_Score

    ndg_Score['normalised_ndg_score'] = ndg_Score['normalised_ndg_score'].apply(lambda x: 0 if x < 0 else x)
    ndg_Score['normalised_ndg_score'] = ndg_Score['normalised_ndg_score'].apply(lambda x: 800 if x > 800 else x)

    ndg_Score['new_Level'] = pd.cut(ndg_Score['normalised_ndg_score'],
                               bins=[-1, 250,400,800],
                               labels=[1, 2, 3])

    ndg_Score['Score'] = ndg_Score['normalised_ndg_score']
    ndg_Score['Level'] = ndg_Score['new_Level']

    ndg_Score.drop(columns=['normalised_ndg_score','new_Level'],inplace=True)


    #########################################
    # supplier_customer_detail.to_csv('./final_run/supplier_customer_detail.csv',header=True,index=True)
    # Agg_supplier_score.to_csv('./final_run/Agg_supplier_score.csv',header=True,index=True)
    # Agg_customer_score.to_csv('./final_run/Agg_customer_score.csv',header=True,index=True)
    # ndg_Score.to_csv('./final_run/ndg_Score.csv',header=True,index=True)

    #########################################




    Customer_Supplier_Score = supplier_customer_detail.copy()
    file = "Customer_Supplier_Score"
    file1 = '"Customer_Supplier_Score"'

    cs = cnn.cursor()

    cs.execute("SHOW TABLES LIKE 'Customer_Supplier_Score' ")
    pd.set_option('display.max_columns', 30)
    print(Customer_Supplier_Score.head())
    print(Customer_Supplier_Score.columns)
    if cs.fetchone():
        cs.execute("SHOW TABLES LIKE 'Audit_Customer_Supplier_Score' ")
        print("Inside Audit_Customer_Supplier_Score")
        if not cs.fetchone():
            # Create the audit table if it doesn't exist
            print("Inside Audit_Customer_Supplier_Score if")
            cs.execute("""
                CREATE TABLE "Audit_Customer_Supplier_Score" AS
                SELECT *
                FROM "Customer_Supplier_Score"
            """)
        else:
            # Stack new data onto existing records
            print("Inside Audit_Customer_Supplier_Score else")
            cs.execute("""
                INSERT INTO "Audit_Customer_Supplier_Score"
                SELECT *
                FROM "Customer_Supplier_Score"
            """)

    cs.execute("""DROP TABLE IF EXISTS """ + file1 + """""")
    df = Customer_Supplier_Score
    i = 10
    df1 = df.head(i)
    df1.to_sql(file, if_exists='append', con=engine, index=False)

    if i <= len(df):
        rest_of_df = df.iloc[i:]
        rest_of_df.to_csv(
            '/home/ubuntu/Result/output.csv', index=False)

        cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
    """)
    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file1 + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)
    cnn.commit()
    os.remove('/home/ubuntu/Result/output.csv')

    # In[45]:

    file = "Agg_supplier_score"
    file1 = '"Agg_supplier_score"'

    cs = cnn.cursor()

    cs.execute("SHOW TABLES LIKE 'Agg_supplier_score' ")
    if cs.fetchone():
        print("Inside Audit_Agg_supplier_score")
        cs.execute("SHOW TABLES LIKE 'Audit_Agg_supplier_score' ")

        if not cs.fetchone():
            # Create the audit table if it doesn't exist
            print("Inside Audit_Agg_supplier_score if")
            cs.execute("""
                CREATE TABLE "Audit_Agg_supplier_score" AS
                SELECT *
                FROM "Agg_supplier_score"
            """)
        else:
            # Stack new data onto existing records
            print("Inside Audit_Agg_supplier_score else")
            cs.execute("""
                INSERT INTO "Audit_Agg_supplier_score"
                SELECT *
                FROM "Agg_supplier_score"
            """)

    cs.execute("""DROP TABLE IF EXISTS """ + file1 + """""")
    df = Agg_supplier_score
    i = 10
    df1 = df.head(i)
    df1.to_sql(file, if_exists='append', con=engine, index=False)

    if i <= len(df):
        rest_of_df = df.iloc[i:]
        rest_of_df.to_csv(
            '/home/ubuntu/Result/output.csv', index=False)

        cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
    """)
    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file1 + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)
    cnn.commit()
    os.remove('/home/ubuntu/Result/output.csv')

    # In[46]:
    # import pandas as pd
    print(Agg_customer_score)
    print(Agg_customer_score.columns)
    file = "Agg_customer_score"
    file1 = '"Agg_customer_score"'

    cs = cnn.cursor()

    cs.execute("SHOW TABLES LIKE 'Agg_customer_score' ")
    if cs.fetchone():
        print("Inside Audit_Agg_customer_score")
        cs.execute("SHOW TABLES LIKE 'Audit_Agg_customer_score' ")

        if not cs.fetchone():
            print("Inside Audit_Agg_customer_score if")
            # Create the audit table if it doesn't exist
            cs.execute("""
                CREATE TABLE "Audit_Agg_customer_score" AS
                SELECT *
                FROM "Agg_customer_score"
            """)
        else:
            print("Inside Audit_Agg_customer_score else")
            # Stack new data onto existing records
            cs.execute("""
                INSERT INTO "Audit_Agg_customer_score"
                SELECT *
                FROM "Agg_customer_score"
            """)

    cs.execute("""DROP TABLE IF EXISTS """ + file1 + """""")
    df = Agg_customer_score
    i = 10
    df1 = df.head(i)
    df1.to_sql(file, if_exists='append', con=engine, index=False)

    if i <= len(df):
        rest_of_df = df.iloc[i:]
        rest_of_df.to_csv(
            '/home/ubuntu/Result/output.csv', index=False)

        cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
    """)
    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file1 + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)
    cnn.commit()
    os.remove('/home/ubuntu/Result/output.csv')

    # In[47]:

    file = "ndg_Score"
    file1 = '"ndg_Score"'

    cs = cnn.cursor()
    cs.execute("SHOW TABLES LIKE 'ndg_Score' ")
    if cs.fetchone():
        print("Inside Audit_ndg_Score")
        cs.execute("SHOW TABLES LIKE 'Audit_ndg_Score' ")

        if not cs.fetchone():
            print("Inside Audit_ndg_Score if")
            # Create the audit table if it doesn't exist
            cs.execute("""
                CREATE TABLE "Audit_ndg_Score" AS
                SELECT *
                FROM "ndg_Score"
            """)
        else:
            print("Inside Audit_ndg_Score else")
            # Stack new data onto existing records
            cs.execute("""
                INSERT INTO "Audit_ndg_Score"
                SELECT *
                FROM "ndg_Score"
            """)

    cs.execute("""DROP TABLE IF EXISTS """ + file1 + """""")
    df = ndg_Score
    i = 10
    df1 = df.head(i)
    df1.to_sql(file, if_exists='append', con=engine, index=False)

    if i <= len(df):
        rest_of_df = df.iloc[i:]
        rest_of_df.to_csv(
            '/home/ubuntu/Result/output.csv', index=False)

        cs.execute("""
        PUT 'file:///home/ubuntu/Result/output.csv' @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
    """)
    cs.execute("""
        COPY INTO CREDIT_SCORE_PHASE2.psi.""" + file1 + """
        FROM @CREDIT_SCORE_PHASE2.psi.%""" + file1 + """
        FILE_FORMAT = (TYPE = CSV COMPRESSION = AUTO SKIP_HEADER = 1 FIELD_DELIMITER = "," RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY ='\042')
    """)
    cnn.commit()
    os.remove('/home/ubuntu/Result/output.csv')
