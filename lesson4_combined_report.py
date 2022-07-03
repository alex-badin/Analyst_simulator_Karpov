# import libs
import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io

# clickhouse connecton parameters. same for all requests
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220520',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# TG bot parameters
chat_id = 269002297
bot = telegram.Bot(token='5322845139:AAF4zchpncemr0VGcVgA_YCO3RsncQJR0m0')

## FEED+MESSENGER COMBINED REPORT
# get data for combined report on usage of messenger and feed (3 columns for each type of usage)
query_combined = '''
select  day, countIf(user_type = 'feed only') feed_only
, countIf( user_type = 'messenger only') messenger_only, countIf( user_type = 'both') both
from
(select user_id, day, sum(type), multiIf(sum(type)=0.5, 'feed only', sum(type)=1.0, 'messenger only', 'both') as user_type
from
(
select user_id, day, any(type)
from 
  (select user_id , toDate(time) day, 1.0 as type
  from simulator_20220520.message_actions
  UNION ALL
  select user_id , toDate(time) day, 0.5 as type
  from simulator_20220520.feed_actions
  )
  group by user_id, day, type
)
where day > today()-15 and day<today()
group by user_id, day)
group by day
'''
df_combined = ph.read_clickhouse(query_combined, connection=connection)

# convert to date (otherwise unattractive date_time format on chart)
df_combined['day'] = df_combined['day'].dt.date

# assign values to type of usage variables
feed_only = df_combined['feed_only'].iloc[-1]
messenger_only = df_combined['messenger_only'].iloc[-1]
both = df_combined['both'].iloc[-1]
total_dau = feed_only + messenger_only + both

# sql request for messegenger data
query_mess = '''SELECT toDate(time) day, count() N_messages, count(DISTINCT user_id) N_users
from simulator_20220520.message_actions 
where toDate(time)<today() and toDate(time)>today()-15
group by toDate(time)
'''
df_mess = ph.read_clickhouse(query_mess, connection=connection)

# values for messenger report
dau_mess = df_mess['N_users'].iloc[-1]
n_messages = df_mess['N_messages'].iloc[-1]
end_date = df_mess['day'].astype(str).iloc[-1]

# text for combined report
report_combined = f'''
<b>Combined report</b> for yesterday ({end_date}):
DAU of the app = {total_dau}, among which {messenger_only} used messenger only, {feed_only} used feed only and {both} used both.
<b>Messenger app</b>
DAU = {dau_mess}, messages sent = {n_messages}
To get detailed updates on <i>Feed service</i> please subscribe for the respective report.
'''
bot.sendMessage(chat_id=chat_id, text=report_combined, parse_mode=telegram.ParseMode.HTML)

# chart for combined report (DAU by type of usage) & send to TG
fig_comb_rep = df_combined[-7:].plot(x='day', stacked=True, kind='bar', figsize=(10,5), title='Types of users dynamics');
comb_report = io.BytesIO()
fig_comb_rep.figure.savefig(comb_report)
comb_report.seek(0)
bot.send_photo(photo=comb_report, chat_id=chat_id)

# chart for messneger report & send to TG
fig_mess_rep = df_mess[-7:].plot(x='day', title='Messenger dynamics, last 7 days', figsize=(10,5))
mess_report = io.BytesIO()
fig_mess_rep.figure.savefig(mess_report)
mess_report.seek(0)
bot.send_photo(photo=mess_report, chat_id=chat_id)
