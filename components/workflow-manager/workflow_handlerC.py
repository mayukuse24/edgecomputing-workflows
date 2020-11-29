import requests
import time
import timeit
import random
import uuid
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from bson import Binary

import docker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from workflow import Workflow
from component import Component
import converter

# TODO: switch to base class and inherit for each workflow
class WorkflowHandler():
    def __init__(self):
        self.swarm_client = docker.from_env()
        self.http_session = self._create_http_session()
        self.persist_name_to_component = {}
        self.workflow_id_to_obj = {}
        self.request_id_to_component_data = {}

        # Starting mongodb on workflow manager startup
        component = Component("mongodb", True, None)
        component.deploy(self.swarm_client, mounts=["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"])
        self.persist_name_to_component["mongodb"] = component

        mongo_url = "mongodb://10.176.67.87:{port}".format(port=component.target_port)
        component.client = MongoClient(mongo_url)

    def _create_http_session(self):
        '''
        Using same session for every instance of workflow. This helps to reduce
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

    def aggregate(self, query, workflow_id):
        agg_query = [
            { "$match": { "workflow_id": workflow_id } },
            { "$group": { "_id": "$is_threat", "count": { "$sum": 1 } } }
        ]

        agg_results = self.aggregate_db("workflow", "result", agg_query)

        print('Result from aggregation', agg_results)   

        threat_summary = {"total": 0}
        for group in agg_results:
            threat_summary["total"] += int(group["count"])
            if group["_id"] == True:
                threat_summary["threats"] = int(group["count"])

        return {"summary": threat_summary}

    # TODO: move db functions into mongodb interface
    def insert_db(self, db_name, collection_name, data):
        print("Writing result into mongo db")
        db_component = self.persist_name_to_component["mongodb"]

        for i in range(5):
            try:
                db = db_component.client[db_name]

                collection = db[collection_name]

                collection.insert_one(data)

                return
            except:
                time.sleep(pow(2,i))

                print("Connection failed while writing into DB, Retrying...")
                #return #ideally exit function call with failure message to componenet

    def aggregate_db(self, db_name, collection_name, query):
        db_component = self.persist_name_to_component["mongodb"]

        for i in range(5):
            try:
                db = db_component.client[db_name]

                collection = db[collection_name]

                result = list(collection.aggregate(query))

                return result
            except AutoReconnect:
                time.sleep(pow(2, i))

                print("Connection failed while reading from DB, Retrying...")
            except Exception as e:
                print(e, 'Error aggregating result from mongodb')

                raise e

    def reduce_dataflow(self, component_to_output, workflow_id, request_id):
        '''
        TODO: make this function dynamic, allow to be passed by client
        '''
        audio_analysis_resp = component_to_output.get("audio_analysis", None)
        text_keywords_resp = component_to_output.get("text_keywords", None)
        text_classification_resp = component_to_output.get("text_classification", None)
        speech_to_text_resp = component_to_output.get("speech_to_text", None)

        threat_level = 0 # ranges between 0 to 100

        if audio_analysis_resp is not None:
            audio_threat = audio_analysis_resp.get('Inaccuracy', 0)
            threat_level = max(threat_level, audio_threat)

        if text_keywords_resp is not None:
            sentiments = text_keywords_resp.get('Sentence Sentiments', [0])

            # Select negative sentiments (0-1) in % obtained on text
            flat_sentiments = [sublist[1] for sublist in sentiments]

            sem_threat = 0
            if len(flat_sentiments) > 0:
                sem_threat = (sum(flat_sentiments) / len(flat_sentiments)) * 100

            threat_level = max(threat_level, sem_threat)

        if text_classification_resp is not None:
            results = text_classification_resp.get('results', [])

            classify_threat = 0
            if len(results) > 0:
                pred_buckets = results[0].get('predictions', {})

                classify_threat = max(list(pred_buckets.values())) * 100

            threat_level = max(threat_level, classify_threat)

        is_threat = False
        if threat_level > 50:
            is_threat = True

        # Inserting to mongodb automatically adds _id key with a new ObjectId value
        # Using request_id as unique identifier instead of mongodb assigned id 
        threat_resp = {
            '_id': str(request_id),
            'workflow_id': workflow_id,
            'speech_text': speech_to_text_resp,
            'threat_level': int(threat_level),
            'is_threat': is_threat
        }

        return threat_resp

    def run_dataflow(self, workflow_id, data):
        request_id = uuid.uuid4()

        component_to_output = {}

        print("Processing request for workflow id {}, request id {}".format(workflow_id, request_id))

        workflow_obj = self.workflow_id_to_obj.get(workflow_id)

        dataflow = workflow_obj.dataflow

        bfs_queue = []
        for component_name in dataflow["root"]:
            # Add element to bfs queue with info of next component and data to be used to run it
            bfs_queue.append({
                "caller": "root",
                "callee": component_name,
                "current_data": {"data": data},
                "current_type": 0
            })
        
        while len(bfs_queue) > 0:
            node_obj = bfs_queue.pop(0)

            caller_name = node_obj["caller"]
            callee_name = node_obj["callee"]
            component = workflow_obj.name_to_component[callee_name]
            current_data = node_obj["current_data"]
            current_type = node_obj["current_type"]

            print(
                "Handling dataflow from {} to {} with data {} and type {}".format(
                    caller_name, callee_name, current_data, current_type
            ))

            # Fetch required converter and convert to input expected by component
            if current_type == -1 or component.input_type == -1:
                input_data = current_data # Do not convert
            else:
                try:
                    convert_func = converter.TYPE_TO_CONVERTER[current_type][component.input_type]

                    input_data = convert_func(current_data)
                except KeyError as ke:
                    print("Converter not found for {} type {} to {}".format(
                        callee_name,
                        current_type,
                        component.input_type
                    ))

                    raise ke

            print("Converted data", input_data)

            start = timeit.default_timer()

            # Send request to run component
            if callee_name == "mongodb":
                resp = self.insert_db("workflow", caller_name, input_data)
            else:
                resp = component.run(input_data)

            stop = timeit.default_timer()

            print("component_execution_time:{} {} secs".format(callee_name, stop - start))

            # Save intermediate output
            component_to_output[callee_name] = resp

            # Add components to receive output data in bfs queue
            children = dataflow.get(callee_name, [])

            for child_name in children:
                child = workflow_obj.name_to_component[child_name]

                bfs_queue.append({
                    "caller": callee_name,
                    "callee": child.name,
                    "current_data": resp,
                    "current_type": component.output_type
                })


        # Run reduction code
        result = self.reduce_dataflow(component_to_output, workflow_id, request_id)

        # Save final result in mongodb
        # TODO: convert this to a db interface or Component child class
        # TODO: switch db name to workflow type or by tenant
        self.insert_db("workflow", "result", result)
        
        return result

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
                component = Component(component_name, is_persist, self.http_session)

                component.deploy(self.swarm_client)

                if is_persist:
                    self.persist_name_to_component[component_name]

            # Add component to map
            name_to_component[component_name] = component

        workflow_obj.name_to_component = name_to_component

        self.workflow_id_to_obj[workflow_id] = workflow_obj