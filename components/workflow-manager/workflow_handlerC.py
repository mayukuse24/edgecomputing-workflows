import requests
import time
import random
import uuid
from pymongo import MongoClient
from bson import Binary

import docker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from Workflow import Workflow
from Component import Component

# TODO: switch to base class and inherit for each workflow
class WorkflowHandler():
    def __init__(self):
        self.swarm_client = docker.from_env()
        self.http_session = self._create_http_session()
        self.persist_name_to_component = {}
        self.workflow_id_to_obj = {}

        print("Testing if function runs twice")

        # Starting mongodb on workflow manager startup
        component = Component("mongodb", True)
        component.deploy(self.swarm_client, mounts=["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"])
        self.persist_name_to_component["mongodb"] = component

    def _create_http_session(self):
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

    def _send_request(self, app_port, path, json=None, files=None):
        # TODO: use domain name instead of ips
        return self.http_session.post(
                'http://10.176.67.87:{port}{path}'.format(port=app_port, path=path),
                json=json,
                files=files
            ).json()

    def run_dataflow_a(self, specs, input_data, workflow_id):
        request_id = uuid.uuid4()

        mongo_url = "mongodb://10.176.67.87:{port}".format(port=specs['mongodb']['port'])
        
        # TODO: add retry and backoff on connection failure
        client = MongoClient(mongo_url)

        db_speech = client["speech"]
        speech_table = db_speech["speech_to_text"]

        # TODO: Send request to component in order one-by-one and transform result
        # as required for next component

        print("Sending payload to convert speech to text")
        payload = {"file": input_data}
        resp = self._send_request(specs['speech']['port'], '/speech_to_text', files=payload)
        print("Speech to text response", resp)
        audio_to_text_data = resp['text']

        print("Sending payload to analyse audio for tone")
        payload = {"file": input_data}
        audioanalysis_resp = self._send_request(specs['audio_analysis']['port'], '/audio_analysis', files=payload)
        print("Audio analysis response", audioanalysis_resp)

        print("Sending payload to obtain keywords from input")
        payload = {'data': audio_to_text_data}
        textsem_resp = self._send_request(specs['text_keywords']['port'], '/text_keywords', json=payload)
        print("Text keywords response", textsem_resp)

        try:
            print("Storing audio and text response in mongo database")
            output = speech_table.insert_one({"text": audio_to_text_data, "audio": Binary(bytes(input_data))})
            print("Data pushed to speech db... ", str(output))
        except:
            print("Connection error, mongo service is not up")
            
        print("Sending payload to compress input data")
        payload = {"type": "gzip","data": audio_to_text_data}
        resp = self._send_request(specs['compression']['port'], '/compress', json=payload)
        print("Compression response", resp)

        print("Sending payload to classify text")
        payload = {
            "text": [
                audio_to_text_data
            ]
        }
        textclassify_resp = self._send_request(specs['text_classification']['port'], '/model/predict', json=payload)
        print("Text classification response", textclassify_resp)

        # TODO: calculate final threat level based on component results
        threat_level = 0 # ranges between 0 to 100

        if audioanalysis_resp is not None:
            audio_threat = audioanalysis_resp.get('Inaccuracy', 0)
            threat_level = max(threat_level, audio_threat)

        if textsem_resp is not None:
            sentiments = textsem_resp.get('Sentence Sentiments', [0])

            flat_sentiments = [sentiment for sublist in sentiments for sentiment in sublist]

            sem_threat = 0
            if len(flat_sentiments) > 0:
                sem_threat = (sum(flat_sentiments) / len(flat_sentiments)) * 100

            threat_level = max(threat_level, sem_threat)

        if textclassify_resp is not None:
            results = textclassify_resp.get('results', [])

            classify_threat = 0
            if len(results) > 0:
                pred_buckets = results[0].get('predictions', {})

                classify_threat = max(list(pred_buckets.values())) * 100

            threat_level = max(threat_level, classify_threat)

        is_threat = False
        if threat_level > 50:
            is_threat = True

        db = client["workflow-a"]
        results_table = db["results"]

        # Inserting to mongodb automatically adds _id key with a new ObjectId value
        # Using request_id as unique identifier instead of mongodb assigned id 
        threat_resp = {
            '_id': str(request_id),
            'workflow_id': workflow_id,
            'speech_text': audio_to_text_data,
            'threat_level': int(threat_level),
            'is_threat': is_threat
        }

        print("Storing threat result in database", threat_resp)

        try:    
            output = results_table.insert_one(threat_resp)
            print("Stored result in workflow-a db... ")
        except Exception as e:
            print(e, 'Error writing result to mongodb')

        # TODO: call aggregator/mongodb to aggregate past threat results for workflow_id
        agg_query = [
            { "$match": { "workflow_id": workflow_id } },
            { "$group": { "_id": "$is_threat", "count": { "$sum": 1 } } }
        ]

        try:
            agg_results = list(results_table.aggregate(agg_query))
        except Exception as e:
            print(e, 'Error aggregating result from mongodb')

        print('Result from aggregation', agg_results)   

        threat_summary = {"total": 0}
        for group in agg_results:
            threat_summary["total"] += int(group["count"])
            if group["_id"] == True:
                threat_summary["threats"] = int(group["count"])

        return {'result': threat_resp, "summary": threat_summary}

    def run_dataflow(self, workflow_id, data):
        request_id = uuid.uuid4()

        print("Processing request for workflow id {}, request id {}".format(workflow_id, request_id))

        workflow_obj = self.workflow_id_to_obj.get(workflow_id)

        dataflow = workflow_obj.dataflow

        bfs_queue = []
        for component_name in dataflow["root"]:
            bfs_queue.append({
               "name": component_name,
               "current_data": {"data": data},
               "current_type": 0
            })
        
        while len(bfs_queue) > 0:
            node_obj = bfs_queue.pop(0)

            component = workflow_obj.name_to_component[component_name]
            current_data = node_obj["current_data"]
            current_type = node_obj["current_type"]

            # TODO: Fetch required converter and convert to input expected by component

            # TODO: Send request to run component

            # TODO: Save intermediate output

            # TODO: Put component children in bfs queue

        # TODO: Run reduction code

        # TODO: Save final result in mongodb
        
        return {}

    def create_workflow(self, workflow_id, dataflow, is_persist):
        workflow_obj = Workflow(workflow_id, dataflow)

        # Picks out unique component names from all components mentioned in dataflow object
        component_names = list(set(list(dataflow.keys()) + [component_name for sublist in dataflow.values() for component_name in sublist]))

        # TODO: Verify dataflow - components, converters

        name_to_component = {}
        for component_name in component_names:
            # Skip for dataflow keywords
            if component_name in ["root"]:
                continue

            component = None

            # Reuse component if persist
            if is_persist and component_name in self.persist_name_to_component:
                component = self.persist_name_to_component[component_name]

            # Components that are pre-deployed as persistant
            if component_name in ["mongodb"]:
                component = self.persist_name_to_component[component_name]

            # Deploy component for non-persist case or first time persist
            if component is None:
                component = Component(component_name, is_persist)

                component.deploy(self.swarm_client)

                if is_persist:
                    self.persist_name_to_component[component_name]

            # Add component to map
            name_to_component[component_name] = component

        workflow_obj.name_to_component = name_to_component

        self.workflow_id_to_obj[workflow_id] = workflow_obj