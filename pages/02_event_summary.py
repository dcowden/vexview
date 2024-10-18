import streamlit as st
import controller
import opr
import polars as pl
import plotly.express as px
from great_tables import loc, style

st.title("Event Summary")
DATE_FORMAT="%m/%d/%Y"
event_list = controller.get_events()

box_options = event_list.select(['id','name','start']).with_columns(
    pl.concat_str([
        pl.col('start').dt.strftime(DATE_FORMAT),
        pl.col('name'),
    ],separator=" ").alias('label')
).sort('start',descending=True)


def display_value(event_id):
    return box_options.filter(pl.col('id') == event_id).get_column('label')[0]

selected_event_id = st.selectbox("ChooseEvent", options=event_list.get_column('id'),format_func=display_value)

selected_event_id = event_list.filter(pl.col('id') == selected_event_id).get_column('id')[0]


matches = controller.get_matches().filter(
    pl.col('event_id') == selected_event_id
)
matches_for_display = matches.sort(['round','matchnum'],descending=False).select([
    'name','field','red_0_name','red_1_name',
    'red_score','blue_0_name','blue_1_name','blue_score']
)

rankings = controller.get_rankings().filter(
    pl.col('event_id') == selected_event_id
)
rankings_for_display  = rankings.select([
    'team_name','rank','wins','losses',
    'ties','wp','ap','sp','high_score','average_points','total_points'
]).sort(['rank'])

all_teams = controller.get_teams()
oprs =opr.calculate_opr_ccwm_dpr(matches)

teams_with_opr = oprs.join(all_teams,how='inner',left_on='team_id',right_on='id').with_columns(
    pl.concat_str(['team_name','organization'],separator=' - ').alias('team_name')
   ).select([
    'team_id','number','team_name','opr','dpr','ccwm'
]).sort(['opr'],descending=True)

teams_and_ranks = teams_with_opr.join(rankings,how='inner',left_on='team_id',right_on='team_id')



col1, col2 = st.columns(2)

with col1:
    st.header("Avg Points vs Rank")
    plot = px.scatter(rankings, x='average_points', y='rank', hover_name='team_name', size='average_points')
    st.plotly_chart(plot)

    st.header("Avg Points vs OPR")
    plot = px.scatter(rankings, x='average_points', y='opr', hover_name='team_name', size='opr')
    st.plotly_chart(plot)

with col2:
    st.header("Rank Vs OPR")
    plot = px.scatter(teams_and_ranks, x='rank', y='opr', hover_name='team_name', size='opr')
    st.plotly_chart(plot)

st.subheader('team OPRs')
st.dataframe(teams_with_opr)

st.subheader('matches')
st.dataframe(matches_for_display,height=800)

st.subheader('rankings')
st.dataframe(rankings,height=700)
