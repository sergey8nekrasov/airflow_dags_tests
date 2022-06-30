#!/usr/bin/env python
# coding: utf

import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data_nekrasov():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top10_domain_to_nekrasov():
    # Найти топ-10 доменных зон по численности доменов
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['zone'] = [val[-1] for val in top_doms.domain.str.split('.')]
    top_10_domain = top_doms.groupby('zone', as_index=False) \
    .agg({'rank': 'count'}) \
    .rename(columns={'rank': 'count'}) \
    .sort_values('count', ascending=False) \
    .head(10)
    # return top_10_domain
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(index=False, header=False))

# Находим список с самыми динными именами основных доменов
def get_longest_domain_to_nekrasov():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_doms['main_domain'] = [val[-2] for val in top_doms.domain.str.split('.')]
    top_doms['domain_name_len'] = top_doms.main_domain.str.len()
    longest_domain = top_doms.query('domain_name_len == domain_name_len.max()')[['main_domain', 'domain_name_len']] \
        .sort_values('main_domain')
    # return longest_domain
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

# На каком месте находится домен airflow.com?
# Зададим функцию поиска и вовда ранка: сначала по полному совпадению
# Если не находит, то по любым совпадениям имени домена (без зоны)
def airflow_search_nekrasov():
    top_doms = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    if top_doms.query('domain == "airflow.com"')[['rank', 'domain']].shape[0] > 0:
        airflow_rank = top_doms.query('domain == "airflow.com"')[['rank', 'domain']]
    else:
        airflow_rank = top_doms.loc[top_doms.domain.str.contains("airflow")][['rank', 'domain']]
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False, header=False))

        
        
def print_data_nekrasov(ds):
    with open('top_10_domain.csv', 'r') as f:
        all_data_top_10 = f.read()
    with open('longest_domain.csv', 'r') as f:
        all_data_longest_domain = f.read()
    with open('airflow_rank.csv', 'r') as f:
        all_data_airflow_rank = f.read()
    date = ds

    print(f'Top 10 domains by count for date {date}')
    print(all_data_top_10)

    print(f'Longest domain name for date {date}')
    print(all_data_longest_domain)

    print(f'Airflow rank for date {date}')
    print(all_data_airflow_rank)
    


default_args = {
    'owner': 's-nekrasov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
}
schedule_interval = '0 8 * * *'

dag = DAG('Unbelievable_DAG_nekrasov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data_nekrasov',
                    python_callable=get_data_nekrasov,
                    dag=dag)

t2_top = PythonOperator(task_id='get_top10_domain_to_nekrasov',
                    python_callable=get_top10_domain_to_nekrasov,
                    dag=dag)

t2_ln = PythonOperator(task_id='get_longest_domain_to_nekrasov',
                        python_callable=get_longest_domain_to_nekrasov,
                        dag=dag)

t2_air = PythonOperator(task_id='airflow_search_nekrasov',
                        python_callable=get_longest_domain_to_nekrasov,
                        dag=dag)

t3 = PythonOperator(task_id='print_data_nekrasov',
                    python_callable=print_data_nekrasov,
                    dag=dag)

t1 >> [t2_top, t2_ln, t2_air] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)









