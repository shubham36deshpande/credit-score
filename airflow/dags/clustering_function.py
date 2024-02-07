#!/usr/bin/env python
# coding: utf-8
######################
# Clustering function: 
# input: Takes data from crm_customers and crm_classes and generates score
# output: Generates 8 temperory tables and then concats everything into one table named     all_customer_supplier_score
# ####################
import random
import os
import snowflake
import snowflake.connector
from sqlalchemy import create_engine, VARCHAR, TIMESTAMP, text
from snowflake.sqlalchemy import URL
import mysql.connector
import pandas as pd
import numpy as np
import math
import datetime
from datetime import datetime, timedelta, date
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings('ignore')
import boto3
import json

def clustering_function():

    # # User Centric Parameter
    # client = boto3.client('secretsmanager', region_name='us-east-1')
    # response = client.get_secret_value(SecretId='stg/snowflake/secrets')

    # Parse the secret value.
    # secret_string = response['SecretString']
    # secret_dict = json.loads(secret_string)
    # account = secret_dict['account']
    # user = secret_dict['user']
    # password = secret_dict['password']
    # database = secret_dict['db']
    # role = secret_dict['role']
    # schema = secret_dict['schema']
    # warehouse = secret_dict['warehouse']
    
    account = 'blb89802.us-east-1'
    # user = 'RGUPTA'
    # password = 'Rachana.growexx@112'
    user='devops'
    password='qd7Lt76K2d1y'
    
    database = 'CREDIT_SCORE_PHASE2'
    schema = 'psi'
    warehouse = 'credit_score_wh'
    role = 'ACCOUNTADMIN'
    
    cnx = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema,
        ocsp_response_cache_filename="/tmp/ocsp_response_cache"
    )

    engine = create_engine(URL(user=user, password=password, account=account,
                           warehouse=warehouse, database=database, schema=schema, role=role))
    cnn = snowflake.connector.connect(user=user, password=password, account=account,
                                      warehouse=warehouse, database=database, schema=schema, role=role)

    date_query = (
        """ select processing_date from "Agg_customer_score" limit 1 """)
    process_date = pd.read_sql(date_query, cnx)

    print(process_date)
    df = process_date.loc[0]["PROCESSING_DATE"]
    print(type(df))
    my_date = df
    print(my_date)

    constant_values = pd.read_sql('select * from constant_values;',cnx)
    constant_values.columns = constant_values.columns.str.lower()
    
    c3_value = constant_values[constant_values['id']=='c3'].values[0][1]
    c4_value = constant_values[constant_values['id']=='c4'].values[0][1]
    c5_value = constant_values[constant_values['id']=='c5'].values[0][1]

    connection = engine.connect()

    query8 = ("select * from psi_raw_data.psi.crm_classes")

    crm_classes = pd.read_sql(query8, cnx)
    crm_classes.columns = crm_classes.columns.str.lower()
    crm_classes.rename(columns={'supplier': 'Supplier'}, inplace=True)
    crm_classes['supplier_id'] = pd.to_numeric(crm_classes['supplier_id'], errors='coerce')
    

    query9 = ("select * from psi_raw_data.psi.crm_customers")
    crm_customers = pd.read_sql(query9, cnx)
    crm_customers.columns = crm_customers.columns.str.lower()
    crm_customers.rename(columns={'customer': 'Customer'}, inplace=True)
    crm_customers['customer_id'] = pd.to_numeric(crm_customers['customer_id'] , errors='coerce')
    crm_customers['ndg_id'] = pd.to_numeric(crm_customers['ndg_id'], errors='coerce')

    crm_classes['jkey'] = 1
    crm_customers['jkey'] = 1

    cs = cnn.cursor()
    Agg_customer_score = pd.read_sql_query(
        f"""SELECT * FROM {database}.{schema}."Agg_customer_score" """, cnn)
    Agg_supplier_score = pd.read_sql_query(
        f"""SELECT * FROM {database}.{schema}."Agg_supplier_score" """, cnn)
    Customer_Supplier_Score = pd.read_sql_query(
        f"""SELECT * FROM {database}.{schema}."Customer_Supplier_Score" """, cnn)
    Agg_customer_score.rename(columns={'CUSTOMER_ID': 'customer_id', 'NDG_ID': 'ndg_id',
                                       'NDG_NAME': 'ndg_name', 'PROCESSING_DATE': 'processing_date'}, inplace=True)
    Agg_supplier_score.rename(columns={'SUPPLIER_ID': 'supplier_id', 'STATUS_INDEX': 'status_index',
                                       'STATUS': 'status', 'PROCESSING_DATE': 'processing_date'}, inplace=True)
    Customer_Supplier_Score.rename(columns={'SUPPLIER_ID': 'supplier_id', 'STATUS_INDEX': 'status_index', 'STATUS': 'status',
                                            'CUSTOMER_ID': 'customer_id', 'NDG_ID': 'ndg_id', 'NDG_NAME': 'ndg_name', 'PROCESSING_DATE': 'processing_date'}, inplace=True)

    
    Agg_customer_score['customer_id'] = pd.to_numeric(Agg_customer_score['customer_id'], errors='coerce')
    Agg_customer_score['ndg_id'] = pd.to_numeric(Agg_customer_score['ndg_id'], errors='coerce')
    
    Agg_supplier_score['supplier_id'] = pd.to_numeric(Agg_supplier_score['supplier_id'], errors='coerce')
    
    
    Customer_Supplier_Score['ndg_id'] = pd.to_numeric(Customer_Supplier_Score['ndg_id'], errors='coerce')
    Customer_Supplier_Score['customer_id'] = pd.to_numeric(Customer_Supplier_Score['customer_id'], errors='coerce')
    Customer_Supplier_Score['supplier_id'] = pd.to_numeric(Customer_Supplier_Score['supplier_id'], errors='coerce')
    
    customer_mean = Agg_customer_score['Score'].mean()
    customer_std = Agg_customer_score['Score'].std()

    supplier_mean = Agg_supplier_score['Score'].mean()
    supplier_std = Agg_supplier_score['Score'].std()

    combine_mean = Customer_Supplier_Score['Score'].mean()
    combine_std = Customer_Supplier_Score['Score'].std()

    cluster_list = [(0, round(len(crm_customers)/8)), (round(len(crm_customers)/8), round(len(crm_customers)/4)), (round(len(crm_customers)/4), round((len(crm_customers)*3)/8)), (round((len(crm_customers)*3)/8), round(len(crm_customers)/2)), (round(
        (len(crm_customers)*4)/8), round((len(crm_customers)*5)/8)), (round((len(crm_customers)*5)/8), round((len(crm_customers)*6)/8)), (round((len(crm_customers)*6)/8), round((len(crm_customers)*7)/8)), (round((len(crm_customers)*7)/8), round((len(crm_customers)*8)/8))]

    cluster_count = 0

    cs = cnn.cursor()
    table_names = ['"All_customer_supplier_score1"', '"All_customer_supplier_score2"', '"All_customer_supplier_score3"', '"All_customer_supplier_score4"',
                   '"All_customer_supplier_score5"', '"All_customer_supplier_score6"', '"All_customer_supplier_score7"', '"All_customer_supplier_score8"']

    # Drop tables
    for table_name in table_names:
        drop_query = f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}"
        cs.execute(drop_query)
        cnn.commit()
    print('Existed All_customer_supplier_score table has been removed...')

    for customer_loop_lower, customer_loop_upper in cluster_list:

        crm_customers1 = crm_customers.iloc[customer_loop_lower:customer_loop_upper]
        All_customer_score = pd.DataFrame()
        All_supplier_score = pd.DataFrame()
        All_customer_supplier_score = pd.DataFrame()

        cluster1 = pd.DataFrame()
        cluster2 = pd.DataFrame()
        cluster3 = pd.DataFrame()
        cluster4 = pd.DataFrame()

        All_customer_score = pd.merge(crm_customers1[['ndg_id', 'ndg_name', 'customer_id', 'Customer', 'customer_type', 'customer_type_id']], Agg_customer_score[[
                                      'customer_id', 'Score', 'Level']], on='customer_id', how='left')
        All_supplier_score = pd.merge(crm_classes[['supplier_id', 'Supplier', 'status_index', 'status']], Agg_supplier_score[[
                                      'supplier_id', 'Score', 'Level']], on='supplier_id', how='left')

        cross_join = pd.merge(crm_customers1, crm_classes,
                              on='jkey').drop('jkey', axis=1)

        All_customer_supplier_score = pd.merge(cross_join, Customer_Supplier_Score[[
                                               'customer_id', 'supplier_id', 'Score', 'Level']], on=['customer_id', 'supplier_id'], how='left')

        All_customer_supplier_score['Score'] = All_customer_supplier_score['Score'].fillna(
            -1)
        All_customer_supplier_score['ndg_name'] = All_customer_supplier_score['ndg_name'].fillna(
            'None')
        All_customer_supplier_score['ndg_id'] = All_customer_supplier_score['ndg_id'].fillna(
            -1)

        All_customer_score.rename(
            columns={'Score': 'customer_score'}, inplace=True)
        All_supplier_score.rename(
            columns={'Score': 'supplier_score'}, inplace=True)
        All_customer_supplier_score.rename(
            columns={'Score': 'combined_score'}, inplace=True)

        All_customer_supplier_score = All_customer_supplier_score.merge(
            Agg_customer_score[['customer_id', 'Score']], on=['customer_id'], how='left')

        All_customer_supplier_score = All_customer_supplier_score.merge(
            Agg_supplier_score[['supplier_id', 'Score']], on=['supplier_id'], how='left')

        All_customer_supplier_score.rename(
            columns={'Score_x': 'customer_score', 'Score_y': 'supplier_score'}, inplace=True)

#         cluster1 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] >= 1) & (
#             All_customer_supplier_score['combined_score'] < 400)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]
#         cluster2 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] >= 400) & (
#             All_customer_supplier_score['combined_score'] < 500)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]
#         cluster3 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] >= 500) & (
#             All_customer_supplier_score['combined_score'] < 700)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]
#         cluster4 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] >= 700) & (
#             All_customer_supplier_score['combined_score'] <= 800)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]

#         centroid1 = round(
#             cluster1[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()
#         centroid2 = round(
#             cluster2[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()
#         centroid3 = round(
#             cluster3[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()
#         centroid4 = round(
#             cluster4[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()

#         centroids = [centroid1, centroid2, centroid3, centroid4]

    # ------------------------------------------------------------------------------------------------
        cluster1 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] >= 0) & (
            All_customer_supplier_score['combined_score'] <= 250)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]
        cluster2 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] > 250) & (
            All_customer_supplier_score['combined_score'] <= 400)][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]
        cluster3 = All_customer_supplier_score[(All_customer_supplier_score['combined_score'] > 400) & (
            All_customer_supplier_score['combined_score'] <=800 )][['customer_id', 'supplier_id', 'combined_score', 'customer_score', 'supplier_score']]


        centroid1 = round(
            cluster1[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()
        centroid2 = round(
            cluster2[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()
        centroid3 = round(
            cluster3[['combined_score', 'customer_score', 'supplier_score']].mean()).to_list()


        centroids = [centroid1, centroid2, centroid3]
    
    # ------------------------------------------------------------------------------------------------

        filtered_df = All_customer_supplier_score[All_customer_supplier_score['combined_score'] == -1].copy()

        if len(filtered_df) != 0:

            filtered_df.loc[filtered_df['customer_score'].isna(), 'customer_score'] = round(
                customer_mean + ((c3_value)*customer_std))
            filtered_df.loc[filtered_df['supplier_score'].isna(), 'supplier_score'] = round(
                supplier_mean + ((c4_value)*supplier_std))
            filtered_df.loc[filtered_df['combined_score'] == -1, 'combined_score'] = round(
                combine_mean + ((c5_value)*combine_std))

            distances = np.sqrt(np.square(filtered_df[[
                                'combined_score', 'customer_score', 'supplier_score']].values[:, None] - centroids).sum(axis=2))

            # Assign distances to DataFrame
            filtered_df['distance1'] = distances[:, 0]
            filtered_df['distance2'] = distances[:, 1]
            filtered_df['distance3'] = distances[:, 2]
#             filtered_df['distance4'] = distances[:, 3]

#             filtered_df['cluster'] = filtered_df[['distance1', 'distance2',
#                                                   'distance3', 'distance4']].idxmin(axis=1).apply(lambda x: int(x[-1]))


            filtered_df['cluster'] = filtered_df[['distance1', 'distance2',
                                                  'distance3']].idxmin(axis=1).apply(lambda x: int(x[-1]))

#             for centroid_i in range(1, 5):
            for centroid_i in range(1, 4):
                condition = filtered_df['cluster'] == centroid_i
                # Select the centroid based on the index value
                centroid = centroids[centroid_i-1]
                filtered_df.loc[condition, ['combined_score', 'customer_score', 'supplier_score']] = round(
                    (filtered_df.loc[condition, ['combined_score', 'customer_score', 'supplier_score']].add(centroid))*0.5)

        All_customer_supplier_score.update(filtered_df)

        All_customer_supplier_score = All_customer_supplier_score.drop(
            'Level', axis=1)
        
        average_supplier_scores = All_customer_supplier_score.groupby('Supplier')['supplier_score'].mean()
        average_customer_scores = All_customer_supplier_score.groupby('Customer')['customer_score'].mean()
        All_customer_supplier_score['supplier_score'] = All_customer_supplier_score['Supplier'].map(average_supplier_scores)
        All_customer_supplier_score['customer_score'] = All_customer_supplier_score['Customer'].map(average_customer_scores)
        
#         All_customer_supplier_score['combined_level'] = pd.cut(All_customer_supplier_score['combined_score'],
#                                                                bins=[-1, 399,
#                                                                      499, 699, 800],
#                                                                labels=[1, 2, 3, 4])
#         All_customer_supplier_score['customer_level'] = pd.cut(All_customer_supplier_score['customer_score'],
#                                                                bins=[-1, 399,
#                                                                      499, 699, 800],
#                                                                labels=[1, 2, 3, 4])
#         All_customer_supplier_score['supplier_level'] = pd.cut(All_customer_supplier_score['supplier_score'],
#                                                                bins=[-1, 399,
#                                                                      499, 699, 800],
#                                                                labels=[1, 2, 3, 4])


        # To make sure there are no combined score with value as 0.
        All_customer_supplier_score.loc[All_customer_supplier_score['combined_score']== 0, 'combined_score'] = 50

# --------------------------------------------------------------------------------------------------------
        All_customer_supplier_score['combined_level'] = pd.cut(All_customer_supplier_score['combined_score'],
                                                               bins=[-1, 250,400,800],
                                                               labels=[1, 2, 3])
        All_customer_supplier_score['customer_level'] = pd.cut(All_customer_supplier_score['customer_score'],
                                                               bins=[-1, 250,400,800],
                                                               labels=[1, 2, 3])
        All_customer_supplier_score['supplier_level'] = pd.cut(All_customer_supplier_score['supplier_score'],
                                                               bins=[-1, 250,400,800],
                                                               labels=[1, 2, 3])
    
# --------------------------------------------------------------------------------------------------------
        All_customer_supplier_score['processing_date'] = my_date


        print('loading data to snowflake...')
        cluster_count = cluster_count+1
        if cluster_count == 1:
            file = "All_customer_supplier_score1"
            file1 = '"All_customer_supplier_score1"'
        elif cluster_count == 2:
            file = "All_customer_supplier_score2"
            file1 = '"All_customer_supplier_score2"'
        elif cluster_count == 3:
            file = "All_customer_supplier_score3"
            file1 = '"All_customer_supplier_score3"'
        elif cluster_count == 4:
            file = "All_customer_supplier_score4"
            file1 = '"All_customer_supplier_score4"'
        elif cluster_count == 5:
            file = "All_customer_supplier_score5"
            file1 = '"All_customer_supplier_score5"'
        elif cluster_count == 6:
            file = "All_customer_supplier_score6"
            file1 = '"All_customer_supplier_score6"'
        elif cluster_count == 7:
            file = "All_customer_supplier_score7"
            file1 = '"All_customer_supplier_score7"'
        elif cluster_count == 8:
            file = "All_customer_supplier_score8"
            file1 = '"All_customer_supplier_score8"'

        cs = cnn.cursor()

        df = All_customer_supplier_score
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
        print('Loading is successfully completed...')

    query10 = (f"""CREATE or replace TABLE All_customer_supplier_score AS 
        SELECT * FROM {database}.{schema}."All_customer_supplier_score1"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score2"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score3"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score4"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score5"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score6"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score7"
        union all
        SELECT * FROM {database}.{schema}."All_customer_supplier_score8"
        """)

    # Execute the union query
    cs = cnn.cursor()
    cs.execute(query10)
    cnn.commit()

    cs = cnn.cursor()

    # Drop tables
    for table_name in table_names:
        drop_query = f"DROP TABLE IF EXISTS {database}.{schema}.{table_name}"
        cs.execute(drop_query)
        cnn.commit()
    print('clustering process is successfully completed...')

    cs.execute("BEGIN;")

    # Execute the first query
    query11 = """
            UPDATE "ALL_CUSTOMER_SUPPLIER_SCORE" AS t1
            SET t1."CUSTOMER_SCORE" = t2."Score",
                t1."CUSTOMER_LEVEL" = t2."Level"
            FROM "Agg_customer_score" AS t2
            WHERE t1."CUSTOMER_ID" = t2."CUSTOMER_ID";
        """
    cs.execute(query11)

    # Execute the second query
    query12 = """
            UPDATE "ALL_CUSTOMER_SUPPLIER_SCORE" AS t1
            SET t1."SUPPLIER_SCORE" = t2."Score",
                t1."SUPPLIER_LEVEL" = t2."Level"
            FROM "Agg_supplier_score" AS t2
            WHERE t1."SUPPLIER_ID" = t2."SUPPLIER_ID";
        """
    cs.execute(query12)

    # Commit the transaction
    cs.execute("COMMIT;")
    print('Retrieve and updated customer & supplier score for the customer with historical data...')

    # -----------------------------------------------------------------------------------------------------------------


    # import mysql.connector
    # import pandas as pd
    # import numpy as np
    # import math
    # import datetime
    # from datetime import datetime, timedelta, date
    # from sklearn.preprocessing import MinMaxScaler
    # import warnings
    # warnings.filterwarnings('ignore')
    # # import snowflake.connector
    # from snowflake.sqlalchemy import URL
    # from sqlalchemy import create_engine, VARCHAR, TIMESTAMP, text
    # import os
    # import random

    def min_score(level):
        if level == 1:
            return 0
        elif level == 2:
            return 251
        elif level == 3:
            return 401     
        
    account = 'blb89802.us-east-1'
    # user = 'RGUPTA'
    # password = 'Rachana.growexx@112'

    user='devops'
    password='qd7Lt76K2d1y'
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
    
    
    
    engine = create_engine(URL(account=account, user=user, password=password,
                       database=db, schema=schema, warehouse=warehouse, role=role))
    cnn = snowflake.connector.connect(user=user, password=password, account=account,
                                      warehouse=warehouse, database=database, schema=schema, role=role)
    connection = engine.connect()


    updated_levels = pd.read_sql_query(
        'SELECT * FROM CUSTOMER_UPDATE_LEVEL', cnn)
    
    sorted_df = updated_levels.sort_values('UPDATED_AT', ascending=False)
    updated_levels = sorted_df.drop_duplicates('CUSTOMER_ID', keep='first')
    updated_levels['CUSTOMER_ID'] = updated_levels['CUSTOMER_ID'].astype(float)
    updated_levels['NEW_LEVEL'] = updated_levels['NEW_LEVEL'].astype(int)

    hist_cust_ids = pd.read_sql("""select Distinct(customer_id) from "Agg_customer_score";""", cnx)
    hist_cust_ids = hist_cust_ids['CUSTOMER_ID'].values
    
    print("Customers for level update :: ",len(updated_levels['CUSTOMER_ID'].unique()))
    print(updated_levels['CUSTOMER_ID'].unique().tolist())
    
    # Create a cursor
    cursor = cnn.cursor()
        
    if not updated_levels.empty:
        customer_list_update_levels = updated_levels['CUSTOMER_ID'].unique(
        ).tolist()
        for i in customer_list_update_levels:
            if i not in hist_cust_ids:
                
                level_info = updated_levels[updated_levels['CUSTOMER_ID'] == i]['NEW_LEVEL'].iloc[0]
                min_level_score = min_score(level_info) + 50
                
                print('Updating customer with fixed weights : Non-Historical/New', i)
                
                # print("\t Previous score and level")
                # print('\t\t',pd.read_sql(f"select * from all_customer_supplier_score where customer_id={i};",cnn)[['CUSTOMER_ID','CUSTOMER_SCORE','CUSTOMER_LEVEL']].values[0])
    
    
                # Step 1: Select the rows to update
                select_query = f"select * from all_customer_supplier_score where customer_id={i};"
                cursor.execute(select_query)
                selected_rows = cursor.fetchall()
    
                # Step 2: Update the selected rows
                update_query = f"""
                    UPDATE all_customer_supplier_score
                    SET CUSTOMER_SCORE = {min_level_score},
                        Customer_LEVEL = {level_info}
                    WHERE customer_id = {i};
                """
                cursor.execute(update_query)
    
                # Commit the changes
                cnn.commit()
                
                # print("\t New score and level")
                # print('\t\t',pd.read_sql(f"select * from all_customer_supplier_score where customer_id={i};",cnn)[['CUSTOMER_ID','CUSTOMER_SCORE','CUSTOMER_LEVEL']].values[0])
    
    # Close the cursor and connection
    cursor.close()
    
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
    

    engine = create_engine(URL(account=account, user=user, password=password,
                       database=db, schema=schema, warehouse=warehouse, role=role))
    cnn = snowflake.connector.connect(user=user, password=password, account=account,
                                      warehouse=warehouse, database=database, schema=schema, role=role)
    
    connection = engine.connect()

    updated_levels = pd.read_sql_query(
        'SELECT * FROM SUPPLIER_UPDATE_LEVEL', cnn)
    
    sorted_df = updated_levels.sort_values('UPDATED_AT', ascending=False)
    updated_levels = sorted_df.drop_duplicates('SUPPLIER_ID', keep='first')
    updated_levels['SUPPLIER_ID'] = updated_levels['SUPPLIER_ID'].astype(float)
    updated_levels['NEW_LEVEL'] = updated_levels['NEW_LEVEL'].astype(int)
    
    print("Suppliers for level update :: ",len(updated_levels['SUPPLIER_ID'].unique()))
    print(updated_levels['SUPPLIER_ID'].unique().tolist())

    hist_supp_ids = pd.read_sql("""select Distinct(supplier_id) from "Agg_supplier_score";""", cnx)
    hist_supp_ids = hist_supp_ids['SUPPLIER_ID'].values

    # Create a cursor
    cursor = cnn.cursor()
        
    if not updated_levels.empty:
        supplier_list_update_levels = updated_levels['SUPPLIER_ID'].unique(
        ).tolist()
        for i in supplier_list_update_levels:
            if i not in hist_supp_ids:
                
                level_info = updated_levels[updated_levels['SUPPLIER_ID'] == i]['NEW_LEVEL'].iloc[0]
                min_level_score = min_score(level_info) + 50
                
                print('Updating supplier with fixed weights : Non-Historical/New', i)
                
                # print("\t Previous score and level")
                # print('\t\t',pd.read_sql(f"select * from all_customer_supplier_score where supplier_id={i};",cnn)[['SUPPLIER_ID','SUPPLIER_SCORE','SUPPLIER_LEVEL']].values[0])
    
    
                # Step 1: Select the rows to update
                select_query = f"select * from all_customer_supplier_score where supplier_id={i};"
                cursor.execute(select_query)
                selected_rows = cursor.fetchall()
    
                # Step 2: Update the selected rows
                update_query = f"""
                    UPDATE all_customer_supplier_score
                    SET SUPPLIER_SCORE = {min_level_score},
                        Supplier_LEVEL = {level_info}
                    WHERE supplier_id = {i};
                """
                cursor.execute(update_query)
    
                # Commit the changes
                cnn.commit()
                
                # print("\t New score and level")
                # print('\t\t',pd.read_sql(f"select * from all_customer_supplier_score where supplier_id={i};",cnn)[['SUPPLIER_ID','SUPPLIER_SCORE','SUPPLIER_LEVEL']].values[0])
    
    # Close the cursor and connection
    cursor.close()
    
# clustering_function()
