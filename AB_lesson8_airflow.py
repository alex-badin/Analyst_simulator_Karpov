
import pandas as pd
import pandahouse
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.badin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 14),
}

# Интервал запуска DAG
schedule_interval = "0 23 * * *"

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report():

    @task
    def extract_feed():
        q_feed = '''
        select user_id
        , gender
        , age
        , os
        , countIf(action = 'view') as views
        , countIf(action = 'like') as likes
        from 
        (SELECT user_id, action
        , multiIf(age<25, '25-', age>=25 and age < 35, '25-35', '35+') as age
        , If(gender = 0, 'male', 'female') as gender
        , os
        from simulator_20220520.feed_actions
        where toDate(time) = today()-1
        ) 
        GROUP by user_id, age, gender, os
        '''

        cube_feed = pandahouse.read_clickhouse(q_feed, connection=connection)
        return cube_feed
    
    @task
    def extract_mess():
        q_mess = '''
        select *
        from
        (select user_id, count(user_id) messages_sent
        from simulator_20220520.message_actions 
        GROUP by user_id
        having toDate(time) = today()-1) t1
        full join 
        (select reciever_id as user_id, count() messages_received
        from simulator_20220520.message_actions 
        GROUP by reciever_id
        having toDate(time) = today()-1) t2
        using(user_id)'''

        cube_mess = pandahouse.read_clickhouse(q_mess, connection = connection)
        return cube_mess
    

    @task
    def trans_comb(cube_feed, cube_mess):
        cube_comb = cube_feed.merge(cube_mess, on='user_id')
        return cube_comb

    @task
    def trans_gender(cube_comb):
        by_gender = cube_comb.groupby('gender')\
            ['views', 'likes', 'messages_sent','messages_received'].sum()
        by_gender['variable'] = 'gender'
        return by_gender

    @task
    def trans_age(cube_comb):
        by_age = cube_comb.groupby('age')\
            ['views', 'likes', 'messages_sent','messages_received'].sum()
        by_age['variable'] = 'age'
        return by_age

    @task
    def trans_os(cube_comb):
        by_os = cube_comb.groupby('gender')\
            ['views', 'likes', 'messages_sent','messages_received'].sum()
        by_os['variable'] = 'os'
        return by_os

    @task
    def trans_final(by_gender, by_age, by_os):
        final_table = pd.concat([by_gender, by_age, by_os])
        final_table.rename(columns={'index':'value'}, inplace=True)
        final_table['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
        return final_table

    @task
    def load_table(final_table):
        connection_load = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '656e2b0c9c',
            'user': 'student-rw',
            'database': 'test'
        }

        print(final_table)
        print('creating table...')
        q_create = '''CREATE TABLE IF NOT EXISTS test.AB_Table_report2 
                    (date Date, variable String, value String, views UInt64, likes UInt64, messages_sent UInt64, messages_received UInt64)
                    ENGINE = Log()'''
        pandahouse.execute(connection=connection_load, query=q_create)
        print('table created. Sending to database')
        pandahouse.to_clickhouse(final_table, 'AB_Table_report2', connection=connection_load, index=False)
        print('Database updated.')

    cube_feed = extract_feed()
    cube_mess = extract_mess()
    cube_comb = trans_comb(cube_feed, cube_mess)
    by_gender = trans_gender(cube_comb)
    by_age = trans_age(cube_comb)
    by_os = trans_os(cube_comb)
    final_table = trans_final(by_gender, by_age, by_os)
    load_table(final_table)

dag_report = dag_report()