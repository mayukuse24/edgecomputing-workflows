import sys
import glob
import requests
import time
import random
import json

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
    if len(sys.argv) < 3:
        sys.exit("Not enough arguments")

    http_session = create_http_session()

    audio_files = glob.glob('./test/*.wav')
    
    req_type = sys.argv[1]

    req_total = int(sys.argv[2])

    # Fetch dataflow structure
    dataflow_file = open('dataflow/{}'.format(sys.argv[3]), 'r')

    dataflow = json.load(dataflow_file)

    dataflow_file.close()

    client_id = sys.argv[4]

    workflow_id = None

    if len(sys.argv) >= 6:
        workflow_id = sys.argv[5]

    req_params = {}

    if not workflow_id:
        if req_type == 'persist':
            req_params['persist'] = 'true'

        resp = send_request(
            http_session,
            7003,
            '/workflow',
            params=req_params,
            json=dataflow
        )
    
        print("Deployed workflow", resp)

        workflow_id = resp['id']

    req_params['workflow_id'] = workflow_id

    #exit(1)

    #fixed = ['test/weather_speech.wav', 'test/angry_test_1.wav', 'test/angry_test_3.wav']
    for itr in range(req_total):
        #filename = random.choice(audio_files)
        #filename = fixed[itr]
        filename = audio_files[itr % len(audio_files)]

        audio_data = open(filename, 'rb').read()

        print("Client {} sending {} request no. {} for file {}".format(client_id, req_type, itr, filename))

        try:
            resp = send_request(
                http_session,
                7003,
                '/data',
                params=req_params,
                files={'audio': audio_data}
            )

            print("Response", resp)
        except Exception as ex:
            print("Failed request", ex)


    summary = send_request(
            http_session,
            7003,
            '/aggregate',
            params=req_params
        )

    print(summary)
