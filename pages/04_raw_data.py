import streamlit as st

import controller
st.set_page_config(layout="wide")
st.title("Team Detail")


events = controller.get_events()
matches = controller.get_matches()
rankings = controller.get_rankings()

display_config = {
    'id': st.column_config.NumberColumn(  "Id", format="%d" ),
    'start': st.column_config.DatetimeColumn( 'Start', format="D MMM YYYY, h:mm a"),
    'end': st.column_config.DatetimeColumn('End', format="D MMM YYYY, h:mm a"),
    'started': st.column_config.DatetimeColumn('End', format="D MMM YYYY, h:mm a"),
    "updated_at": st.column_config.DatetimeColumn('End', format="D MMM YYYY, h:mm a"),
    'event_id':st.column_config.NumberColumn(  "Id", format="%d" ),
}

st.header(f"Events ({len(events)})")
st.dataframe(events,column_config=display_config)

st.header(f"Matches  ({len(matches)})")
st.dataframe(matches,column_config=display_config)

st.header(f"Rankings ({len(rankings)})")
st.dataframe(rankings,column_config=display_config)