import sys
import glob
import requests
import time
import random

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_http_session():
    '''
    Using new session for every instance of workflow. This helps to reduce
    no. of tcp connections. It also provides a retry mechanism allowing
    newly created services/containers to start
    '''
    retry_strategy = Retry(
        total=50,
        backoff_factor=2
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    http = requests.Session()
    http.mount("http://", adapter)

    return http

def send_request(http_session, app_port, path, params=None, json=None, files=None):
    # TODO: use domain name instead of ips
    return http_session.post(
            'http://10.176.67.85:{port}{path}'.format(port=app_port, path=path),
            params=params,
            json=json,
            files=files
        ).json()

if __name__ == "__main__": 
    if len(sys.argv) < 4:
        sys.exit("Not enough arguments")

    # Fetch dataflow structure
    dataflow = {
        "dataflow" : {
            "root":[
                "speech_to_text",
                "audio_analysis"
            ],
            "speech_to_text":[
                "text_keywords",
                "text_classification"
            ],
            "text_keywords":[
                "mongodb"
            ]
        }
    }

    http_session = create_http_session()

    audio_files = glob.glob('./test/*.wav')
    
    req_type = sys.argv[1]

    req_total = int(sys.argv[2])

    workflow_id = sys.argv[3]

    url_path = '/workflow/surveil'

    req_params = {'workflow_id': workflow_id}
    if req_type == 'persist':
        req_params['persist'] = 'true'

    resp = send_request(
        http_session,
        7003,
        '/workflow',
        params=req_params,
        json=dataflow
    )
    
    print("Sent request for deploying dataflow", resp)

    workflow_id = resp['id']
    req_params = {'workflow_id': workflow_id}

    #exit(1)

    for itr in range(req_total):
        filename = random.choice(audio_files)
        audio_data = open(filename, 'rb').read()

        print("Sending {} request no. {} for file {}".format(req_type, itr, filename))

        resp = send_request(
            http_session,
            7003,
            '/data',
            params=req_params,
            files={'audio': audio_data}
        )

        print("Response", resp)
