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

## FEED REPORT
# get stats on feed from sql
query_daily_report = '''
SELECT toDate(time) day, count(DISTINCT user_id) N_users, countIf(action='view') N_views, countIf(action='like') N_likes, N_likes / N_views CTR
FROM simulator_20220520.feed_actions
WHERE toDate(time) < today() and toDate(time) > today()-15
GROUP BY toDate(time)
'''
df_feed = ph.read_clickhouse(query_daily_report, connection=connection)

# put data from week ago next to each day by creating 4 new columns (to compare vs previous week on chart) 
cols = ['N_users', 'N_views', 'N_likes', 'CTR']
for col in cols:
    df_feed['{}_prev_week'.format(col)] = df_feed[col].shift(periods=7)

# assign KPI variables for text report
begin_date = df_feed['day'].astype(str).iloc[-7]
end_date = df_feed['day'].astype(str).iloc[-1]
dau = df_feed['N_users'].astype(str).iloc[-1]
views = df_feed['N_views'].astype(str).iloc[-1]
likes = df_feed['N_likes'].astype(str).iloc[-1]
ctr = round(df_feed['CTR'].iloc[-1]*100,1).astype(str)+'%'

# text for feed report & send it
report_dau = f'''
<b>Feed service</b> report for <u>yesterday</u> ({end_date}):
DAU = {dau}
Number of views = {views}
Number of likes = {likes}
CTR = {ctr}
'''
bot.sendMessage(chat_id=chat_id, text=report_dau, parse_mode=telegram.ParseMode.HTML)

# building chart for feed report
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15,10))

fig.suptitle('Feed dynamics for last 7 days', fontsize=20)
df_feed[-7:].plot(y=['N_users', 'N_users_prev_week'], x='day', style=['-','--'],color=['b','grey'], ax=axes[0,0], title='DAU')
df_feed[-7:].plot(y=['N_views', 'N_views_prev_week'], x='day', style=['-','--'],color=['g','grey'], ax=axes[0,1], title='Views')
df_feed[-7:].plot(y=['N_likes', 'N_likes_prev_week'], x='day', style=['-','--'],color=['r','grey'], ax=axes[1,0], title='Likes')
df_feed[-7:].plot(y=['CTR', 'CTR_prev_week'], x='day', style=['-','--'],color=['y','grey'], ax=axes[1,1], title='CTR')
plt.tight_layout() # make chart tight
# initialize io object for chart
feed_report = io.BytesIO()
# save chart to io object & send to TG
fig.figure.savefig(feed_report)
feed_report.name = 'test_plot.png'
feed_report.seek(0)
bot.sendPhoto(chat_id=chat_id, photo=feed_report)

## POSTS REPORT FOR YESTERDAY

# get data for posts in feed
query_posts = '''
SELECT post_id, countIf(action='view') N_views, countIf(action='like') N_likes
FROM simulator_20220520.feed_actions
WHERE toDate(time) = today()-1
GROUP BY post_id
'''
df_posts = ph.read_clickhouse(query_posts, connection=connection)

# find 3 top posts and N of views and convert to list
top3posts = df_posts.sort_values('N_views', ascending=False)[:3][['post_id', 'N_views']].values.tolist()

# make text from the list above
top3posts_txt = ', '.join(str(p)+" ("+str(i)+" views)" for p, i in top3posts)

# text report for posts
report_posts = f'<b>Top3 viewed posts</b> in Feed for <u>yesterday</u> ({end_date}) are: {top3posts_txt}.\nPlease see the full report on posts in the attachment.'

bot.sendMessage(chat_id=chat_id, text=report_posts, parse_mode=telegram.ParseMode.HTML)

# create file for top posts
top20posts = df_posts.sort_values('N_views', ascending=False)[:20]
# save as io object & send file to TG
file_object = io.StringIO()
top20posts.to_csv(file_object)
file_object.name = f'top20posts_{end_date}.csv'
file_object.seek(0)
bot.sendDocument(chat_id=chat_id, document=file_object)

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
<b>Feed app</b>
DAU = {dau}, total views = {views}, total likes = {likes}
<b>Messenger app</b>
DAU = {dau_mess}, messages sent = {n_messages}
To get detailed updates on <i>Feed service</i> please subscribe for the respective report.
'''
bot.sendMessage(chat_id=chat_id, text=report_combined, parse_mode=telegram.ParseMode.HTML)

# chart for combined report & send to TG
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
