import dlt
import vex
import motherduck
import util
import logging
from datetime import datetime

logger = logging.getLogger(__name__)



@dlt.resource(table_name="events", write_disposition='merge', primary_key='id')
def sync_events_source(event_list:list[int]):
    yield from  vex.get_events_by_list(event_list)


@dlt.resource(table_name="teams", write_disposition='merge', primary_key='id')
def sync_teams_source(event_list:list[int]):
    yield from vex.get_teams_from_event_list(event_list)


@dlt.resource(table_name="matches", write_disposition='merge', primary_key='id')
def sync_matches_source(event_list:list[dict]):
    yield from vex.get_matches_from_event_list(event_list,loop_delay_secs=5)


@dlt.resource(table_name="rankings", write_disposition='merge', primary_key='id')
def sync_rankings_source(event_list:list[dict]):
    yield from vex.get_rankings_from_event_list(event_list,loop_delay_secs=5)


def should_sync_events_and_teams(all_events) -> bool:

    events_we_have = motherduck.get_events()
    if events_we_have is None:
        return True

    num_events_we_have = len(events_we_have)
    if num_events_we_have  == len(all_events):
        logger.warning(f"No New Events or Teams, we have them all already {num_events_we_have}/{len(all_events)}")
        return False
    else:
        logger.warning(f"Need to Sync Events{num_events_we_have / len(all_events)}")
        return True



def sync():
    all_events = vex.list_all_south_carolina_events_since(since_date=datetime(2023,1,1,0,0,0))
    all_event_ids = [ event['id'] for event in all_events]

    all_events = set(all_event_ids)
    events_we_have_matches_for = set(motherduck.get_events_to_skip_syncing_matches())
    events_we_have_rankings_for = set(motherduck.get_events_to_skip_syncing_rankings())
    matches_to_sync = list(all_events - events_we_have_matches_for)

    rankings_to_sync = list(all_events - events_we_have_rankings_for)
    logger.warning(f"Need to sync {len(matches_to_sync)} Matches...")
    logger.warning(f"Need to sync {len(rankings_to_sync)} Rankings...")


    pipeline = dlt.pipeline(
        pipeline_name='vex',
        destination='motherduck',
        dataset_name='mann_2025'
    )

    if should_sync_events_and_teams(all_events):
        logger.info("Syncing Events...")
        load_info = pipeline.run(sync_events_source(all_event_ids))
        print(load_info)

        logger.info("Syncing Teams...")
        load_info = pipeline.run(sync_teams_source(all_event_ids))
        print(load_info)

    load_info = pipeline.run(sync_matches_source(matches_to_sync))
    print(load_info)

    load_info = pipeline.run(sync_rankings_source(rankings_to_sync))
    print(load_info)

    #all_matches = vex.get_matches_from_event_list(all_events,loop_delay_secs=2)
    #print(f"Got {len(all_matches)} Matches. From {len(all_events)} Events")





if __name__ == '__main__':
    util.setup_logging()
    sync()

