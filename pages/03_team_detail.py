import streamlit as st
import controller
import polars as pl
st.title("Team Detail")

event_list = controller.get_events()
event_ids = list(event_list.get_column('id'))
col1, col2 = st.columns(2)

selected_event = st.selectbox("ChooseEvent", options=event_ids,)

matches = controller.get_matches().filter(
    pl.col('event_id') == selected_event
)
st.subheader('matches')


if len(matches) > 0:
    st.dataframe(matches)
else:
    st.subheader('No Data')



