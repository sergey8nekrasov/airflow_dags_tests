import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'{"s-nekrasov-21"}') % 23

default_args = {
    'owner': 's-nekrasov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 22),
    'schedule_interval': '0 8 * * *'
}

CHAT_ID = -620798068
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def dag_with_bot_nekrasov():

    @task()
    def get_data(file):
        df = pd.read_csv(data).dropna(axis='index', how='any', subset=['Year'])
        df.Year = df.Year.astype(int)
        task_df = df.query('Year == 2001')  
        return task_df

    @task()
    def get_best_sold(task_df):
        best_sold = task_df.groupby('Name', as_index=False) \
        .agg({'Global_Sales': 'sum'}) \
        .query('Global_Sales == Global_Sales.max()')
        best_sold.to_csv(index=False)
        return best_sold

    @task()
    def get_top_genre_EU(task_df):
        top_genre_EU = task_df.groupby('Genre', as_index=False) \
        .agg({'EU_Sales': 'sum'}) \
        .query('EU_Sales == EU_Sales.max()')
        top_genre_EU.to_csv(index=False)
        return top_genre_EU

    @task()
    def get_top_NA_platform(task_df):
        top_NA_platform = task_df.query('NA_Sales > 1') \
        .groupby(['Platform'], as_index=False) \
        .agg({'Name': 'nunique'}) \
        .query('Name == Name.max()')
        top_NA_platform.to_csv(index=False)
        return top_NA_platform

    @task()
    def get_top_JP_publisher(task_df):
        top_JP_publisher = task_df.groupby('Publisher', as_index=False) \
        .agg({'JP_Sales': 'mean'}) \
        .query('JP_Sales == JP_Sales.max()')
        top_JP_publisher.to_csv(index=False)
        return top_JP_publisher

    @task()
    def get_number_of_games_EU(task_df):
        EU_JP_games = task_df\
            .groupby('Name',as_index=False)\
            .agg({'EU_Sales':sum, 'JP_Sales':sum})
        number_of_games = EU_JP_games[EU_JP_games.EU_Sales > EU_JP_games.JP_Sales].Name.count()
        return number_of_games_EU

    @task(on_success_callback=send_message)
    def print_data(best_sold, top_genre_EU, top_NA_platform, top_JP_publisher, number_of_games_EU):
        print(f'The best-selling game in {year} in the world: {best_sold}')
        print(f'The best-selling genre in {year} in Europe: {top_genre_EU}')
        print(f'Top platform games with a million copie in {year} in NA: {top_NA_platform}')
        print(f'Publishers with the highest average sales in {year} in Japan: {top_JP_publisher}')
        print(f'The number of games sold is better in Europe than in Japan in {year}: {number_of_games_EU}')
        
    data = get_data(file)
    best_sold = get_best_sold(task_df)
    top_genre_EU = get_top_genre_EU(task_df)
    top_NA_platform = get_top_NA_platform(task_df)
    top_JP_publisher = get_top_JP_publisher(task_df)
    number_of_games_EU = get_number_of_games_EU(task_df)  
    print_data(best_sold, top_genre_EU, top_NA_platform, top_JP_publisher, number_of_games_EU)
    dag_with_bot_nekrasov = dag_with_bot_nekrasov()
