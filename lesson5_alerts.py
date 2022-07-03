## import libs and global variables
import pandas as pd
import pandahouse
import telegram
import io

# clickhouse connecton parameters. same in all requests
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220520',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220420'
}

chat_id = 269002297
bot = telegram.Bot(token='5322845139:AAF4zchpncemr0VGcVgA_YCO3RsncQJR0m0')

## load data and check deviations
# query for last 15-minutes period and the same periods for previous N days (to account for daily seasonality).
# N of days to look back
n_days = 20

# sql query for feed
query_feed = f'''
select toStartOfFifteenMinutes(time) ts
, formatDateTime(ts, '%R') as hm
, count(DISTINCT user_id) n_users
, countIf(action='view') n_views
, countIf(action='like') n_likes
, toDate(ts) dates
, dateDiff('day', ts, now()) days_ago
from simulator_20220520.feed_actions
where formatDateTime(toStartOfFifteenMinutes(time),'%R') = formatDateTime(toStartOfFifteenMinutes(now()) - 60*15, '%R')
        and toDate(time) > today() - {n_days}
group by ts
order by days_ago
'''

df_feed = pandahouse.read_clickhouse(query_feed, connection=connection)

# sql query for messenger
query_mess = f'''
select toStartOfFifteenMinutes(time) ts
, formatDateTime(ts, '%R') as hm
, count(DISTINCT user_id) n_mess_users
, count(DISTINCT reciever_id) n_mess_recievers
, count(user_id) n_messeges
, toDate(ts) dates
, dateDiff('day', ts, now()) days_ago
from simulator_20220520.message_actions 
where formatDateTime(toStartOfFifteenMinutes(time),'%R') = formatDateTime(toStartOfFifteenMinutes(now()) - 60*15, '%R')
        and toDate(time) > today() - {n_days}
group by ts
'''

df_mess = pandahouse.read_clickhouse(query_mess, connection=connection)

# combine df's into one for to check for all metrics in one procedure
df_comb = pd.merge(df_feed, df_mess, on='dates', how='outer')

def check_metric(df_prev_days, metric, coef_minor = 0, coef_major = 0):
    coef_minor = coef_minor or 0.1 # N of st deviations (sigmas)
    coef_major = coef_major or 0.3 # N of st deviations (sigmas)
    link_to_dashboard = 'http://superset.lab.karpov.courses/r/1258'
    metric_now = df_prev_days[metric][0]
    metric_mean = df_prev_days[metric].mean()
    metric_std = df_prev_days[metric].std()

    if metric_now > metric_mean + metric_std*coef_major:
        is_alarm = 1
        is_chart = 1
        diff = metric_now / metric_mean - 1
        text_alarm = f'''
        ALERT!!! {metric} surged by {diff:.2%} vs previous days. 
        Investigate it NOW here: {link_to_dashboard}.'''

    elif metric_now < metric_mean - metric_std*coef_major:
        is_alarm = 1
        is_chart = 1
        diff = metric_now / metric_mean - 1
        text_alarm = f'''
        ALERT!!! {metric} dropped by {diff:.2%} vs previous days. 
        Investigate it NOW here: {link_to_dashboard}.'''

    elif metric_now > metric_mean + metric_std*coef_minor or metric_now < metric_mean - metric_std*coef_minor:
        is_alarm = 1
        is_chart = 0
        diff = metric_now / metric_mean - 1
        text_alarm = f'''
        Minor deviation on {metric} by {diff:.2%} vs mean of the previous days for the same time period (more than {coef_minor} sigmas).
        If you have nothing valuable to do you can check it here: {link_to_dashboard}.'''

    else: 
        is_alarm = 0
        is_chart = 0
        text_alarm = 'No warnings'
    return is_alarm, is_chart, text_alarm

## check new data and send alarm if needed

# assign list of metrics. Must be aligned with SQL query. 
metrics = ['n_users', 'n_views', 'n_likes', 'n_mess_users', 'n_mess_recievers', 'n_messeges']
# run thru metrics with check_metric function and send respective info
for metric in metrics:
    is_alarm, is_chart, text_alarm = check_metric(df_comb, metric, coef_minor=0.2, coef_major=1)
    if is_alarm == 1:
        bot.sendMessage(chat_id=chat_id, text=text_alarm, parse_mode=telegram.ParseMode.HTML)
    if is_chart == 1:
        alarm_chart = io.BytesIO()
        time_interval = df_comb['hm_x'][0]
        df_comb.plot(x='dates', y=metric, figsize=(10,5), title=f'{metric} by 15-min periods starting at {time_interval} for different days', legend=False)
        fig.figure.savefig(alarm_chart)
        alarm_chart.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=alarm_chart)