import logging

import streamlit as st
import pipeline
from streamlit_extras.streaming_write import write
from streamlit_extras.capture import logcapture

def run_data_refresh():
    logger = pipeline.everyone_use_the_same_logger()
    logger.setLevel(logging.INFO)
    with logcapture(st.empty().code, from_logger=logger):
        pipeline.set_loop_delay(3)
        pipeline.sync()

if st.button("Refresh data Now"):
    run_data_refresh()
