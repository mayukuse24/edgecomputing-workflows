import requests
import time
import random

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
            total=10,
            backoff_factor=1
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
        
        service = self.swarm_client.services.create(
            image=image,
            name='{name}-{port}'.format(name=name, port=random_port),
            endpoint_spec=endpoint_spec
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

        # TODO: Send request to component in order one-by-one and transform result
        # as required for next component
        payload = {"type": "gzip","data": input_data}

        resp = self._send_request(compress_spec['port'], '/compress', payload)

        # TODO: terminate containers if not persist
        print("Stopping compression service")
        compress_spec['service_obj'].remove()

        return resp


