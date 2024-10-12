import streamlit as st
import controller
import opr
import polars as pl
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




all_teams = controller.get_teams()
oprs =opr.calculate_opr_ccwm_dpr(matches)
teams_with_opr = oprs.join(all_teams,how='inner',left_on='team_id',right_on='id').with_columns(
    pl.concat_str(['team_name','organization'],separator=' - ').alias('team_name')
   ).select([
    'number','team_name','opr','dpr','ccwm'
]).sort(['opr'],descending=True)

st.subheader('teams')
st.dataframe(teams_with_opr)

st.subheader('matches')
st.dataframe(matches_for_display,height=800,column_config={
})
