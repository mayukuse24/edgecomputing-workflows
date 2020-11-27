import requests
import time
import random
from pymongo import MongoClient
from bson import Binary
#import threading
import copy
from time import sleep
import docker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import Workflow
from Workflow import flow,component_node
from Component import Component

#max threads =100
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
    mongo_url = ""
    workflows ={}
    returns = {}
    base_components = []
    used_ports =[]
    used_ids =[]
    db_audio = None
    db_speech = None
    speech_table = None
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
            total=60,
            backoff_factor=2
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        http = requests.Session()
        http.mount("http://", adapter)

        return http

    def _send_request(self, app_port, path, json=None, files=None):
        #print(app_port," ",path," ",files," ",files["file"]," ",type(files)," ",type(files["file"]),"iiii\n")
        # TODO: use domain name instead of ips
        return self.http_session.post(
            'http://0.0.0.0:{port}{path}'.format(port=app_port, path=path),json=json,files=files).json()
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

        print("Starting text keyword service")
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

    def gen_start_db(self,comp:Component,id):
        name = 'mongodb'
        mounts = ["mongodb_mongo-data-1:/data/db", "mongodb_mongo-config-1:/data/configdb"]

        endpoint_spec = docker.types.EndpointSpec(
            ports={comp.target_port[id-1]: comp.internal_port}
        )

        service = self.swarm_client.services.create(
            image=comp.image,
            name='{name}-persist-{port}'.format(name=name, port=comp.target_port[id-1]),
            endpoint_spec=endpoint_spec,
            mounts=mounts
        )
        comp.add_spec(id, service)
        self.mongo_url ="mongodb://0.0.0.0:{port}".format(port=comp.target_port[id-1])
        # TODO: add retry and backoff on connection failure
        client = MongoClient(self.mongo_url)
        self.db_audio = client["audio"]  # using a database named audio
        self.db_speech = client["speech"]
        self.speech_table = self.db_speech["speech_to_text"]


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
        #service.run()

    def gen_init_test(self):

        cmp=Component(5001,"Compression",'mayukuse2424/edgecomputing-compression','/compress')
        cmp.input_type = 4
        cmp.output_type = 5
        self.base_components += [cmp]
        cmp = Component(5050, "Shorten", 'shorten',"/summerize")
        cmp.input_type = 0
        cmp.output_type = 0
        self.base_components += [cmp]
        cmp=Component(5000,'speech', 'mayukuse2424/edgecomputing-speech-to-text','/speech_to_text')
        cmp.input_type = 1
        cmp.output_type = 2
        self.base_components += [cmp]
        cmp = Component(5000, 'text_keywords', 'sayerwer/text_semantics:text_semantics', '/text_keywords')
        cmp.input_type = 0
        cmp.output_type = 23
        self.base_components += [cmp]
        cmp = Component(5005,"audio_analysis",'sayerwer/threataud','/audio_analysis')
        cmp.input_type =1
        cmp.output_type = 6
        self.base_components += [cmp]
        cmp = Component(5000, "classify", 'quay.io/codait/max-toxic-comment-classifier', '/model/predict')
        cmp.input_type = 7
        cmp.output_type = 24
        self.base_components += [cmp]
        cmp = Component(27017, "mongodb", 'mongo', None)
        cmp.input_type = 3
        cmp.output_type = -1
        self.base_components += [cmp]

    def gen_get_flow_id(self):
        return random.randint(0, 65500)
    def gen_flow_init_test_3(self):
        w_test = flow()
        w_test.flow_id = self.gen_get_flow_id()

        nd1 = self.gen_init_comp(0, True, 1, 1) # compression
        nd2 = self.gen_init_comp(2, True, 1, 0) # speech to text
        nd3 = self.gen_init_comp(4, True, 1, 0) # audio analysis
        nd4 = self.gen_init_comp(3, True, 1, 1) # text keywords
        nd5 = self.gen_init_comp(5, True, 1, 1) # classification
        nd6 = self.gen_init_comp(6, False, 1, 1) # database
        nd2.next_set = [nd4, nd5, nd1, nd6]


        w_test.start_components = [nd2,nd3,nd6]
        w_test.flow_id = self.gen_get_flow_id()

        w_test.run_order = [nd6,nd5,nd4,nd3,nd2,nd1]
        return w_test,w_test.flow_id

    def gen_init_comp(self,cmp,temp,inp=1,ord=0):
        idq, _ = self.base_components[int(cmp)].create(self.used_ports, temp)
        nd = component_node(self.base_components[int(cmp)], idq, expected_inputs=inp, order_number=ord)
        nd.set_nodeid(self.used_ids)
        return nd

    def gen_flow_init_test_2(self):
        w_test = flow()

        idq, _ = self.base_components[2].create(self.used_ports, True)
        nd = component_node(self.base_components[2], idq, expected_inputs=1, order_number=0)
        nd.set_nodeid(self.used_ids)
        id2, _ = self.base_components[1].create(self.used_ports, True)
        nd2 = component_node(self.base_components[1], id2, expected_inputs=1, order_number=1)
        nd2.set_nodeid(self.used_ids)
        nd.next_set += [nd2]
        w_test.start_components = [nd]
        w_test.flow_id = 0
        w_test.run_order = [nd2, nd]
        return w_test

    def gen_flow_init_test(self):
        w_test = flow()
        n =1
        #print(len(w_test.start_components),"\n\n\n\n\n\n\n\n\n")
        id,_=self.base_components[1].create(self.used_ports,True)
        nd = component_node(self.base_components[1],id,expected_inputs=1,order_number=0)
        nd.set_nodeid(self.used_ids)
        id2, _ = self.base_components[1].create(self.used_ports, True)
        #print(self.base_components[1].target_port)
        nd2 = component_node(self.base_components[1], id2, expected_inputs=1, order_number=1)
        nd2.set_nodeid(self.used_ids)
        nd.next_set += [nd2]
        w_test.start_components =[nd]
        w_test.flow_id =0
        w_test.run_order = [nd2,nd]
        return w_test


    def start_generic_test(self,wf:flow):
        self.workflows[wf.flow_id] = wf
        self.returns[wf.flow_id] =[]
        for cmp_nd in wf.run_order:
            print(cmp_nd.comp.path,cmp_nd.comp.image, cmp_nd.comp.internal_port)
            if not cmp_nd.stat:
                if cmp_nd.comp.path is not None:
                    #print("test")
                    self.gen_component_start(cmp_nd.comp, cmp_nd.id)
                    #cmp_nd.stat = True
                else:
                    self.gen_start_db(cmp_nd.comp, cmp_nd.id)

    def run_workflow(self, id):
        wf = self.workflows[id]
        count =0
        output =[]

        while True:
            if wf.run_items <len(wf.data):
                count =0
                resp = self.run_pass(wf,wf.data[wf.run_items])
                self.returns[id] += [resp]
                wf.run_items +=1
            else:
                sleep(0.050+count*.50)
                count+=1
        #output
        #return output
    def run_pass(self,wf,data):

        audio_threat = None
        threat_level = 0
        sentiment = None
        flat_sentiments = None
        sem_threat = 0
        results = None
        classify_threat = None
        is_threat = False
        sp_txt =""

        print(data, type(data))
        ##add multipath suport
        unrun_components = sorted(wf.start_components)
        added_ids = []
        count = 0

        for cm in wf.start_components:
            print(cm)
            added_ids += [cm.nodeid]
            cm.give_data(data)
            #if cm.order_number !=0:
                #unrun_components.remove(cm)



        while count < len(unrun_components):
            print(0)
            resp =""
            curr = unrun_components[count]
           # if curr.nodeid not in added_ids:
                #print(77)
                #added_ids += [curr.nodeid]
                #curr.give_data(data)
            print(curr.data)
            #print(2, curr.data[0][0], type(curr.data[0][0]))
            dat = Workflow.convert(curr.data[0][1],curr.comp.input_type,curr.data[0][0])
            print("\n\n\n\n\n\n\n\n",dat)
            print(curr.comp.spec[curr.id]['port'], curr.comp.path,curr.id,curr.comp.input_type)

            if curr.comp.input_type in [0,2,4]:
                resp = self._send_request(curr.comp.spec[curr.id]['port'], curr.comp.path, json=dat)
            elif curr.comp.input_type in [1]:
                #print("test",dat["file"].content_length)
                #dat["file"].close()
                payload = {"file":dat}#dat#
                resp = self._send_request(curr.comp.spec[curr.id]['port'], curr.comp.path, files=payload)
            
            elif curr.comp.input_type in [3]:
                try:
                    print("Storing audio and text response in mongo database")
                    resp = self.speech_table.insert_one(dat)
                    print("Data pushed to speech db... ", str(resp))
                except:
                    print("Connection error, mongo service is not up")
            print(resp,type(resp),"  iooioi ")
            if curr.comp.output_type == 6 and resp is not None:
                audio_threat = resp.get('Inaccuracy',0)
                threat_level = max(threat_level,audio_threat)
            elif curr.comp.output_type == 23 and resp is not None:
                sentiments = resp['Sentence Sentiments']
                print(sentiments,"tjjgiuiyujjjjjjjkl")
                flat_sentiments = [sentiment for sublist in sentiments for sentiment in sublist]
                sem_threat = 0
                if len(flat_sentiments):
                   sem_threat = (sum(flat_sentiments)/len(flat_sentiments))*100
                threat_level = max(threat_level, sem_threat)
            elif curr.comp.output_type == 25 and resp is not None:
                results = resp.get('results',[])
                classify_threat = 0
                if len(results) > 0:
                    pred_buckets = results[0].get('predictions',{})
                    classify_threat = max(list(pred_buckets.values())) *100
                threat_level = max(threat_level, classify_threat)
            elif curr.comp.name == "speech":
                sp_txt = resp["text"]
            #curr.data = []
            #print(resp, "\n\n\n\n\n\n\n\n\n\n")
            for cm in curr.next_set:
                cm.give_data((resp, curr.comp.output_type))
                if cm not in unrun_components or (cm.comp.name == "mongodb" and curr.comp.name != "mongodb"):
                    unrun_components.append(cm)
                    added_ids += [cm.nodeid]
            count += 1
        if threat_level > 50:
            is_threat = True
        client = MongoClient(self.mongo_url)
        dbr = client["workflow"]
        results_table =dbr["results"]
        threat_resp = {
            '_id':str(wf.count)+"-"+str(wf.flow_id),
            'workflow_id':wf.flow_id,
            'speech_text':sp_txt,
            'threat_level':int(threat_level),
            'ist_threat':is_threat
        }
        wf.count +=1
        try:
            output =results_table.insert_one(threat_resp)
        except Exception as e:
            print(e,threat_resp)
        return resp

    def get_data(self,id):
        print("\n\n\n\n\n")
        agg_results = None
        agg_r2 = None
        if id not in self.returns:
            return [{"error":"The requested flow has not been initialized"}]
        try:
            agg = [
                {"$match":{"workflow_id":id}},
                {"$group": {"_id":"$is_threat","count":{"$sum":1}}}
            ]
            agg_2 = [
                {"$match":{"workflow_id":id}},
            ]
            client = MongoClient(self.mongo_url)
            dbr = client["workflow"]
            results_table =dbr["results"]
            agg_results = list(results_table.aggregate(agg))
            agg_r2 = list(results_table.aggregate(agg_2))
            print(agg_results,agg_r2)
        except Exception as e:
            print(e)
        threat_summary = {"total":0}
        for group in agg_results:
            print(group)
            threat_summary["total"] += int(group["count"])
            if group["_id"] == True:
                threat_summary["threats"] = int(group["count"])
        print(threat_summary,agg_r2)
        return {'summary':str(threat_summary),'results':str(agg_r2)}#self.returns[id]

    def gen_output(self,data,id,type):
        print(self.workflows)
        wf = self.workflows[id] #.data += data
        wf.data+= [(data,type)]
        dt = len(wf.data)
        while wf.run_items <dt:
            sleep(.5)

    name_id_map = {
        "compress": 0,
        "speech_to_text": 2,
        "Threat_classifier": 4,
        "text_semantics": 3,
        "classify": 5,
        "mongodb": 6,
        "shorten": 1,
        "aggregator": 7,
    }
    def build_flow(self,components):
        w_test = flow()
        w_test.flow_id = self.gen_get_flow_id()
        print(components)
        build = list(components.keys())
        print(build)

        strt_cmps = components["root"]
        flow_map ={}
        for cmp in strt_cmps:
            nd2 = self.gen_init_comp(self.name_id_map[cmp], True, 1, 0)
            flow_map[cmp] = nd2
        for key, value in components.items():
            if key not in strt_cmps and key not in ["root"]:
                nd2 = self.gen_init_comp(self.name_id_map[key], True, 1, 1)
                flow_map[key] = nd2
        for key, value in components.items():
            if key not in ["root"]:
                tmp = []
                for v in value:
                    nx = flow_map[v]
                    tmp += [nx]
                nc = flow_map[key]
                nc.next_set = tmp
        st_cmp =[]
        for c in strt_cmps:
            if c not in ["root"]:
                st_cmp += [flow_map[c]]
        build_order = []
        for c in build:
            if c not in ["root"]:
                build_order +=[flow_map[c]]
        w_test.start_components = st_cmp
        w_test.run_order = build_order
        return w_test, w_test.flow_id


    def build_flow_pr(self,components):
        w_test = flow()
        w_test.flow_id = self.gen_get_flow_id()
        print(components)
        build = list(components.keys())
        print(build)

        strt_cmps = components["root"]
        flow_map ={}
        for cmp in strt_cmps:
            nd2 = self.gen_init_comp(self.name_id_map[cmp], False, 1, 0)
            flow_map[cmp] = nd2
        for key, value in components.items():
            if key not in strt_cmps and key not in ["root"]:
                nd2 = self.gen_init_comp(self.name_id_map[key], False, 1, 1)
                flow_map[key] = nd2
        for key, value in components.items():
            if key not in ["root"]:
                tmp = []
                for v in value:
                    nx = flow_map[v]
                    tmp += [nx]
                nc = flow_map[key]
                nc.next_set = tmp
        st_cmp =[]
        for c in strt_cmps:
            if c not in ["root"]:
                st_cmp += [flow_map[c]]
        build_order = []
        for c in build:
            if c not in ["root"]:
                build_order +=[flow_map[c]]
        w_test.start_components = st_cmp
        w_test.run_order = build_order
        return w_test, w_test.flow_id


