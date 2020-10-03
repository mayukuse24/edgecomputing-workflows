import requests
import time
import random
from pymongo import MongoClient

import docker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# TODO: switch to base class and inherit for each workflow
class WorkflowHandler():
    def __init__(self):
        self.swarm_client = docker.from_env()
        self.http_session = self._create_http_session() 

    def _create_http_session(self):
        '''
        Using new session for every instance of workflow. This helps to reduce
        no. of tcp connections. It also provides a retry mechanism allowing
        newly created services/containers to start 
        '''
        retry_strategy = Retry(
            total=20,
            backoff_factor=50
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        http = requests.Session()
        http.mount("http://", adapter)

        return http

    def _send_request(self, app_port, path, payload):
        # TODO: use domain name instead of ips
        return self.http_session.post(
            'http://10.176.67.87:{port}{path}'.format(port=app_port, path=path),
            json=payload
        ).json()

    def create_service(self, name, image, internal_port):
        random_port = random.randint(10000, 65500)

        endpoint_spec = docker.types.EndpointSpec(ports={random_port:internal_port})
        
        mount = []
        
        if name == 'mongo':
            mount = ["/data/db:mongodb_mongo-data-1", "/data/configdb/mongodb_mongo-config-1"]
        
        service = self.swarm_client.services.create(
            image=image,
            name='{name}-{port}'.format(name=name, port=random_port),
            endpoint_spec=endpoint_spec
            mounts=mount
        )

        # TODO: Add service spec to map if persist=True
        service_spec = {
            'name': name,
            'port': random_port,
            'service_obj': service
        }

        return service_spec

    def run_workflow_a(self, input_data, persist):
        # TODO: create(if not persist) or retrieve required docker containers
        print("Starting compression service")
        compress_spec = self.create_service('compression', 'mayukuse2424/edgecomputing-compression', 5001)

        print("Starting text classification service")
        classifier_spec = self.create_service('text_classification', 'quay.io/codait/max-toxic-comment-classifier', 5000)
        
        print("Starting text keywordservice")
        text_sem_spec = self.create_service('text_keywords','sayerwer/text_semantics',5000)

        print("Starting mongo service")
        mongo_spec = self.create_service('mongodb', 'mongo', 27017)
        #mongo_url = "mongodb://localhost"
        #client = MongoClient(mongo_url)
        #db = client["audio"] #using a database named audio
	
        #inserted = {"filename":"need to pull name here", "filesize":"14 Zetabytes", "additional details":"will be determined in the future"}
	
        #audio_files = db["files"]
        #output = audio_files.insert_one(inserted)

        #print("Data pushed to db... " + str(output))
        
        # TODO: Send request to component in order one-by-one and transform result
        # as required for next component
        print("Sending payload to compress")

        payload = {"type": "gzip","data": input_data}
        
        test_text_info = {'data':"The cat stretched. Jacob stood on his tiptoes. The car turned the corner. Kelly twirled in circles. She opened the door. Aaron made a picture.\n  I'm sorry. I danced.\n   Sarah and Ira drove to the store.  Jenny and I opened all the gifts.  The cat and dog ate.  My parents and I went to a movie.  Mrs. Juarez and Mr. Smith are dancing gracefully.  Samantha, Elizabeth, and Joan are on the committee.   The mangy, scrawny stray dog hurriedly gobbled down the grain-free, organic dog food."}
        
        text_info_resp = self._send_request(text_sem_spec['port'], '/text_keywords', test_text_info)

        resp = self._send_request(compress_spec['port'], '/compress', payload)

        print("Compression response", resp)

        print("Sending payload to classify")

        payload = {
            "text": [
                "I would like to punch you.",
                "In hindsight, I do apologize for my previous statement."
            ]
        }

        resp = self._send_request(classifier_spec['port'], '/model/predict', payload)

        print("Text classification response", resp)
        
        print("Text Keywords response", text_info_resp)

        # TODO: terminate containers if not persist
        print("Stopping mongo service")
        mongo_spec['service_obj'].remove()
        
        print("Stopping compression service")
        compress_spec['service_obj'].remove()

        print("Stopping text classification service")
        classifier_spec['service_obj'].remove()

        print("Stopping Text Keywords service")
        text_sem_spec['service_obj'].remove()
        return resp


