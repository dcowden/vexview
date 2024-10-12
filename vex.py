from config import current_config
import requests
import json
import logging
from datetime import datetime,date
import time
logger = logging.getLogger(__name__)

DATE_FORMAT="%Y-%m-%d"

VEX_ACCESS_TOKEN=current_config['motherduck']['token']
VEX_BASE_URL= "https://www.robotevents.com/api/v2"
DEFAULT_DIVISION=1

def paginated_robot_events_api_request(path:str, extra_params={},max_pages=100 )-> dict:
    all_data = []
    total_pages = 0
    next_page = path

    while next_page is not None:
        logger.info(f"Fetching {next_page}")
        total_pages+= 1
        r = robot_events_api_request(next_page,extra_params)
        logger.info(f"<Got {len(r['data'])} rows>)")
        all_data.extend(r["data"])
        next_page = r['meta']['next_page_url']
        if total_pages> max_pages:
            break

    return all_data


def robot_events_api_request(path:str , extra_params={}) -> dict:

    base_params = {
        'per_page': 250
    }
    base_params.update(extra_params)
    r = requests.get(path,
        headers={
            'Authorization': f"Bearer {VEX_ACCESS_TOKEN}",
            'accept': 'application/json'},
        params=base_params
    )
    r.raise_for_status()
    return r.json()


def get_fields(from_dict:dict, fields:list[str])-> dict:
    r = {}
    for f in fields:
        if f in from_dict.keys():
            r[f] = from_dict[f]
    return r


def flatten_event(original_event:dict) -> dict:
    n = get_fields(original_event,['id','sku','name','start','end','level','ongoing','awards_finalized'])
    n['season_id'] = original_event['season']['id']
    n['program_id'] = original_event['program']['id']
    return n


def flatten_team(original_team:dict) -> dict:
    n = get_fields(original_team,['id','number','team_name','robot_name','organization','registered','grade','region','postcode'])
    n['city'] = original_team['location']['city']
    n['program_id'] = original_team['program']['id']
    return n


def set_null_if_missing_key(original:dict, fields:list[str]) ->dict:
    r = {}
    r.update(original)

    for f in fields:
        if f not in r:
            r[f] = None
    return r


def flatten_match(original_match:dict) -> dict:
    #TODO; need use jsonpath? something. this sucks
    n = get_fields(original_match,['id','round','instance','matchnum','scheduled','started','field','scored','name','updated_at'])
    n['event_id'] = original_match['event']['id']
    n['event_code'] = original_match['event']['code']
    n['division_id'] = original_match['division']['id']

    for alliance_idx,alliance in enumerate(original_match['alliances']):
        n[f"{alliance['color']}_score"] = alliance['score']
        for team_idx,team in enumerate(alliance['teams']):
            n[f"{alliance['color']}_{team_idx}_id"] = team['team'].get('id',None)
            n[f"{alliance['color']}_{team_idx}_name"] = team['team'].get('name',None)

        #now fix up any keys missing: they would have been skipped over
        set_null_if_missing_key(n,[
            'blue_score',
            'blue_0_id',
            'blue_0_name',
            'blue_1_id',
            'blue_1_name',
            'red_score',
            'red_0_id',
            'red_0_name',
            'red_1_id',
            'red_1_name',
        ])

    return n


def flatten_ranking(original_ranking:dict) -> dict:
    n = get_fields(original_ranking,['id','rank','wins','losses','ties','wp','ap','sp','high_score','average_points','total_points'])
    n['event_id'] = original_ranking['event']['id']
    n['event_name'] = original_ranking['event']['name']
    n['event_code'] = original_ranking['event']['code']
    n['division_id'] = original_ranking['division']['id']
    n['team_id'] = original_ranking['team']['id']
    n['team_name'] = original_ranking['team']['name']
    n['team_code'] = original_ranking['team']['code']
    return n


def get_events_by_list(id_list:list[int]) -> list[dict]:
    r = robot_events_api_request(f"{VEX_BASE_URL}/events", extra_params={
        'id[]': id_list
    })
    return [ flatten_event(x) for x in r['data']]


def list_all_south_carolina_events_since(since_date:datetime=datetime(2023,1,1,0,0)) -> dict:
    logger.info(f"List SC Events")
    r = paginated_robot_events_api_request(f"{VEX_BASE_URL}/events", extra_params={
        'region': 'South Carolina',
        'start' :since_date.isoformat()
    })
    return [ flatten_event(x) for x in r]


def get_teams_from_event_list(event_list:list[int]) -> list[dict]:
    logger.info(f"List Teams for EventList")
    r = paginated_robot_events_api_request(f"{VEX_BASE_URL}/teams", extra_params={
        'event[]': event_list,
        'grade[]': [ 'High School']
    })
    return [ flatten_team(x) for x in r]



def list_events_between(start=datetime, end=datetime) -> dict:
    logger.info (f"List Events, start={start.isoformat()}, end={end.isoformat()}")
    r = paginated_robot_events_api_request(f"{VEX_BASE_URL}/events", extra_params={
        'start' : start.isoformat(),
        'end': end.isoformat()
    })
    return [flatten_event(x) for x in r]


def get_matches_from_event(event_id:int, division_id:int=DEFAULT_DIVISION):
    r =  paginated_robot_events_api_request(f"{VEX_BASE_URL}/events/{event_id}/divisions/{division_id}/matches")
    return [ flatten_match(x) for x in r]


def get_matches_from_event_list(event_list:list[int], loop_delay_secs=5) -> list[dict]:
    all_matches = []
    logger.info(f"List Matches for EventList")
    for event_id in event_list:
        m = get_matches_from_event(event_id)
        all_matches.extend(m)
        time.sleep(loop_delay_secs)

    return all_matches



def get_rankings_from_event(event_id:int, division_id:int=DEFAULT_DIVISION):
    r = paginated_robot_events_api_request(f"{VEX_BASE_URL}/events/{event_id}/divisions/{division_id}/rankings")
    return [ flatten_ranking(x) for x in r]



def get_rankings_from_event_list(event_list:list[int], loop_delay_secs=5) -> list[dict]:
    all_rankings = []
    logger.info(f"List Rankings for EventList")
    for event_id in event_list:
        m = get_rankings_from_event(event_id)
        all_rankings.extend(m)
        time.sleep(loop_delay_secs)

    return all_rankings


def setup_logging():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
                        handlers=[stream_handler])


if __name__ == '__main__':
    setup_logging()

    #all_events = list_all_south_carolina_events()
    #d = flatten_into_dataframe(all_events)
    #print(d)
    #e = pd.DataFrame([ flatten(x) for x in all_events ])
    #print(e)


    #print ( json.dumps(get_events_by_list([50599,51571]),indent=4))
    #all_2024_sc_events =list_all_south_carolina_events_since(since_date=datetime(2024,7,1,0,0,0))
    #all_2024_sc_event_ids = [ x['id'] for x in all_2024_sc_events]
    #print(f"All SC Events={all_2024_sc_event_ids}")
    #all_teams = get_teams_from_event_list(all_2024_sc_event_ids)
    #print(f"Found {len(all_teams)} teams from {len(all_2024_sc_event_ids)} SC Events")
    #print(json.dumps(all_teams,indent=4))
    #matches = get_matches_from_event(56635,1)
    matches = get_matches_from_event(51571, 1)
    print ( json.dumps(matches,indent=4))
    #rankings = get_rankings_from_event(56635,1)
    #print(json.dumps(rankings,indent=4))

    #a = fetch_all_data()
    #describe(a)
    #d = get_rankings_from_event(51571,1)
    #d = list_all_south_carolina_events()
    #d= get_teams_from_event(57443)
    #d=get_teams()
    #flatten_and_print(d)
