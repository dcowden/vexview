import json
import logging
def write_json_to_file(file_path:str, j):
    with open(file_path,'w') as f:
        json.dump(j,f,indent=4)


def pretty_print_json(d: dict):
    print(json.dumps(d,indent=4))


def setup_logging():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
                        handlers=[stream_handler])