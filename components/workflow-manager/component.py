import random
import docker


"""
input/output types defined:
-1 - none
0 - post {data:""}
1 - post {file:...}
2 - post {text:""}
3 - post {text:"",audio:...}    don't convert 
4 - post {type:"",data:""}
5 - post {data_bytes:""}
6 - post {tone info:"",inacuracy:int()}
7 - post {text:[""]}
8 - post {Paragraph Key phrase:[], Sentence Key Words:[], Sentence Sentiments: [[]]}
9 - post [{original_text: "", predictions:{}}]
10 - post {Summarized Text: ""}
"""


COMPONENT_CONFIG_MAP = {
    'compression': {
        'image': 'mayukuse2424/edgecomputing-compression',
        'internal_port': 5001,
        'target_port': 6000,
		'path': '/compress',
		'input_type': 0,
		'output_type': 5
    },
    'mongodb': {
        'image': 'mongo',
        'internal_port': 27017,
        'target_port': 6001,
		'path': None,
		'input_type': -1,
		'output_type': -1
    },
    'speech_to_text': {
        'image': 'mayukuse2424/edgecomputing-speech-to-text',
        'internal_port': 5000,
        'target_port': 6002,
		'path': '/speech_to_text',
		'input_type': 1,
		'output_type': 2
    },
    'text_classification': {
        'image': 'quay.io/codait/max-toxic-comment-classifier',
        'internal_port': 5000,
        'target_port': 6003,
		'path': '/model/predict',
		'input_type': 7,
		'output_type': 9
    },
    'text_keywords': {
        'image': 'sayerwer/text_semantics:text_semantics',
        'internal_port': 5000,
        'target_port': 6004,
		'path': '/text_keywords',
		'input_type': 0,
		'output_type': 8
    },
    'audio_analysis':{
        'image': 'sayerwer/threataud',
        'internal_port': 5005,
        'target_port': 6005,
		'path': '/audio_analysis',
		'input_type': 1,
		'output_type': 6
    },
	'text_summarization':{
        'image': 'mayukuse2424/edgecomputing-summarization',
        'internal_port': 5000,
        'target_port': 6006,
		'path': '/summarize',
		'input_type': 0,
		'output_type': 10
    }
}


class Component:
	used_ports = {}

	def __init__(self, name, is_persist, http_session, mounts=[]):
		config = COMPONENT_CONFIG_MAP[name]

		self.name = name
		self.http_session = http_session
		self.image = config['image']
		self.internal_port = config['internal_port']
		self.path = config['path']
		self.is_persist = is_persist
		self.service_obj = None
		self.target_port = None
		self.input_type = config['input_type']
		self.output_type = config['output_type']
	
	def deploy(self, swarm_client, mounts=[]):
		print("Deploying component", self.name, )
		if self.is_persist:
			target_port = COMPONENT_CONFIG_MAP[self.name]['target_port']

			service_name = '{name}-persist-{port}'.format(name=self.name, port=target_port)
		else:
			target_port = random.randint(10000, 65500)
			while Component.used_ports.get(target_port):
				target_port = random.randint(10000, 65500)

			Component.used_ports[target_port] = True

			service_name = '{name}-temp-{port}'.format(name=self.name, port=target_port)

		self.target_port = target_port

		endpoint_spec = docker.types.EndpointSpec(
            ports={ self.target_port:self.internal_port }
        )

		service = swarm_client.services.create(
            image=self.image,
            name=service_name,
            endpoint_spec=endpoint_spec,
            mounts=mounts
        )

		self.service_obj = service

	def _send_request(self, app_port, path, json=None, files=None):
        # TODO: use domain name instead of ips
		print("Sending request to", self.name, app_port, path, json, files)
		return self.http_session.post(
                'http://10.176.67.87:{port}{path}'.format(port=app_port, path=path),
                json=json,
                files=files
            ).json()

	def run(self, input_data):
		# TODO: Find a better way to handle this
		if self.input_type in [1]:
			return self._send_request(self.target_port, self.path, files=input_data)
		else:
			return self._send_request(self.target_port, self.path, json=input_data)
