import requests
import time
import random
from pymongo import MongoClient
from bson import Binary

import docker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from Workflow import flow,component_node
from Component import Component

COMPONENT_CONFIG_MAP = {
    'compression': {
        'image': 'mayukuse2424/edgecomputing-compression',
        'internal_port': 5001,
        'target_port': 6000
    },
    'mongodb': {
        'image': 'mongo',
        'internal_port': 27017,
        'target_port': 6001
    },
    'speech': {
        'image': 'mayukuse2424/edgecomputing-speech-to-text',
        'internal_port': 5000,
        'target_port': 6002
    },
    'text_classification': {
        'image': 'quay.io/codait/max-toxic-comment-classifier',
        'internal_port': 5000,
        'target_port': 6003
    },
    'text_keywords': {
        'image': 'sayerwer/text_semantics:text_semantics',
        'internal_port': 5000,
        'target_port': 6004
    },
    'audio_analysis':{
        'image': 'sayerwer/threataud',
        'internal_port': 5005,
        'target_port': 6005
    }
}

# TODO: switch to base class and inherit for each workflow
class WorkflowHandler():
    workflows ={}
    base_components = []
    used_ports =[]
    used_ids =[]
    def __init__(self):
        self.swarm_client = docker.from_env()
        self.http_session = self._create_http_session()
        self.persist_service_spec_map = {}

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
        return self.http_session.post('http://0.0.0.0:{port}{path}'.format(port=app_port, path=path),json=json,files=files).json()
        """ return self.http_session.post(
                'http://0.0.0.0:{port}{path}'.format(port=app_port, path=path),
                #'http://10.176.67.87:{port}{path}'.format(port=app_port, path=path),
                json=json,
                files=files
            ).json()"""

    def create_service_temp(self, name, image, internal_port, mounts=[]):
        random_port = random.randint(10000, 65500)

        endpoint_spec = docker.types.EndpointSpec(ports={random_port:internal_port})

        service = self.swarm_client.services.create(
            image=image,
            name='{name}-temp-{port}'.format(name=name, port=random_port),
            endpoint_spec=endpoint_spec,
            mounts=mounts
        )

        # TODO: Add service spec to map if persist=True
        service_spec = {
            'name': name,
            'port': random_port,
            'service_obj': service
        }

        return service_spec

    def create_service_persist(self, name, mounts=[]):
        # Get service if already running
        service_spec = self.persist_service_spec_map.get(name, None)

        if service_spec is not None:
            # TODO: add check to see if service close to termination

            service_spec['last_updated'] = int(time.time() * 1000)

            return service_spec

        service_config = COMPONENT_CONFIG_MAP[name]

        target_port = service_config['target_port']

        endpoint_spec = docker.types.EndpointSpec(
            ports={ target_port:service_config['internal_port'] }
        )

        service = self.swarm_client.services.create(
            image=service_config['image'],
            name='{name}-persist-{port}'.format(name=name, port=target_port),
            endpoint_spec=endpoint_spec,
            mounts=mounts
        )

        service_spec = {
            'name': name,
            'port': target_port,
            'service_obj': service,
            'last_updated': int(time.time() * 1000)
        }

        self.persist_service_spec_map[name] = service_spec

        return service_spec

    def run_dataflow_a(self, specs, input_data):
        mongo_url = "mongodb://10.176.67.87:{port}".format(port=specs['mongodb']['port'])
        
        # TODO: add retry and backoff on connection failure
        client = MongoClient(mongo_url)
        db_audio = client["audio"] #using a database named audio
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
        resp = self._send_request(specs['audio_analysis']['port'], '/audio_analysis', files=payload)
        print("Audio analysis response", resp)

        print("Sending payload to obtain keywords from input")
        payload = {'data': audio_to_text_data}
        resp = self._send_request(specs['text_keywords']['port'], '/text_keywords', json=payload)
        print("Text keywords response", resp)

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
        resp = self._send_request(specs['text_classification']['port'], '/model/predict', json=payload)
        print("Text classification response", resp)

        return resp

    def run_workflow_a_temp(self, input_data):
        print("Starting temporary workflow for audio surveillance")

        # TODO: create required docker containers
        print("Starting speech service")
        #speech_spec = self.create_service_temp('speech', 'codait/max-speech-to-text-converter', 5000)
        speech_spec = self.create_service_temp('speech', 'mayukuse2424/edgecomputing-speech-to-text', 5000)

        print("Starting audio Analysis service")
        thread_spec = self.create_service_temp('audio_analysis', 'sayerwer/threataud', 5005)

        print("Starting text keywordservice")
        text_sem_spec = self.create_service_temp('text_keywords','sayerwer/text_semantics:text_semantics',5000)

        # Note: starting mongo as persistent, since volumes can only be mounted on one db component at a time
        print("Starting mongo service")
        mongo_spec = self.create_service_persist('mongodb', mounts=["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"])

        # print("Starting mongo service")
        # mongo_spec = self.create_service_temp('mongodb', 'mongo', 27017, mounts=["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"])

        print("Starting compression service")
        compress_spec = self.create_service_temp('compression', 'mayukuse2424/edgecomputing-compression', 5001)

        print("Starting text classification service")
        classifier_spec = self.create_service_temp('text_classification', 'quay.io/codait/max-toxic-comment-classifier', 5000)

        resp = self.run_dataflow_a({
            'speech': speech_spec,
            'compression': compress_spec,
            'text_classification': classifier_spec,
            'text_keywords': text_sem_spec,
            'mongodb': mongo_spec,
            'audio_analysis': thread_spec
        }, input_data)

        # TODO: terminate containers
        print("Stopping speech service")
        speech_spec['service_obj'].remove()

        print("Stopping mongo service")
        mongo_spec['service_obj'].remove()

        print("Stopping compression service")
        compress_spec['service_obj'].remove()

        print("Stopping text classification service")
        classifier_spec['service_obj'].remove()

        print("Stopping text keywords service")
        text_sem_spec['service_obj'].remove()

        print("Stopping audio analysis service")
        thread_spec['service_obj'].remove()

        return resp

    def run_workflow_a_persist(self, input_data):
        print("Starting persistant workflow for audio surveillance")

        # TODO: create required components or fetch existing
        print("Starting speech service")
        speech_spec = self.create_service_persist('speech')

        print("Starting mongo service")
        mongo_spec = self.create_service_persist('mongodb', mounts=["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"])

        print("Starting audio Analysis service")
        thread_spec = self.create_service_persist('audio_analysis')

        print("Starting text keywordservice")
        text_sem_spec = self.create_service_persist('text_keywords')

        print("Starting compression service")
        compress_spec = self.create_service_persist('compression')

        print("Starting text classification service")
        classifier_spec = self.create_service_persist('text_classification')

        # TODO: Send request to component in order one-by-one and transform result
        # as required for next component
        resp = self.run_dataflow_a({
            'speech': speech_spec,
            'compression': compress_spec,
            'text_classification': classifier_spec,
            'text_keywords': text_sem_spec,
            'mongodb': mongo_spec,
            'audio_analysis': thread_spec
        }, input_data)

        return resp

    def gen_component_start(self,comp:Component,id):
        #to do make non temp componets work
        print(comp.target_port[id-1])
        endpoint_spec = docker.types.EndpointSpec(ports={comp.target_port[id-1]: comp.internal_port})
        service = self.swarm_client.services.create(
            image=comp.image,
            name='{name}-temp-{port}'.format(name=comp.name, port=comp.target_port[id-1]),
            endpoint_spec=endpoint_spec,
            mounts=comp.mounts
        )
        comp.add_spec(id,service)

    def gen_init_test(self):

        cmp=Component(5001,"Compression",'mayukuse2424/edgecomputing-compression','/compress')
        cmp.input_type = 4
        cmp.output_type = 5
        self.base_components += [cmp]
        cmp = Component(5050, "Shorten", 'shorten',"/summerize")
        cmp.input_type = 0
        cmp.output_type = 0
        self.base_components += [cmp]
    def gen_get_flow_id(self):
        return random.randint(0, 65500)
    def gen_flow_init_test(self):
        w_test = flow()
        n =1
        print(len(w_test.start_components),"\n\n\n\n\n\n\n\n\n")
        id,_=self.base_components[1].create(self.used_ports,True)
        nd = component_node(self.base_components[1],id,expected_inputs=1,order_number=0)
        nd.set_nodeid(self.used_ids)
        id2, _ = self.base_components[1].create(self.used_ports, True)
        print(self.base_components[1].target_port)
        nd2 = component_node(self.base_components[1], id2, expected_inputs=1, order_number=1)
        nd.set_nodeid(self.used_ids)
        nd.next_set += [nd2]
        w_test.start_components =[nd]
        w_test.flow_id =0
        w_test.run_order = [nd2,nd]
        return w_test


    def start_generic_test(self,wf:flow):
        self.workflows[wf.flow_id] = wf
        for cmp_nd in wf.run_order:
            #print(cmp_nd.id,"\n\n\n\n\n\n\n\n\n\n")
            if not cmp_nd.stat:
                self.gen_component_start(cmp_nd.comp, cmp_nd.id)
                #cmp_nd.stat = True



    def gen_dataflow_test(self,data,id):
        print(data,type(data))
        ##add multipath suport
        unrun_components = self.workflows[id].start_components
        added_ids =[]
        count =0
        while count<len(unrun_components):
            print(0)
            curr = unrun_components[count]
            if curr.nodeid not in added_ids:
                added_ids += [curr.nodeid]
            curr.give_data(data)
            print(2,curr.data[0],type(curr.data[0]))
            resp = self._send_request(curr.comp.spec[curr.id]['port'], curr.comp.path, json=curr.data[0])
            print(1)
            curr.data=[]
            print(resp,"\n\n\n\n\n\n\n\n\n\n")
            for cm in curr.next_set:
                cm.give_data(resp)
                if cm.nodeid not in added_ids:
                    unrun_components.append(cm)
                    added_ids += [cm.nodeid]
            count +=1


