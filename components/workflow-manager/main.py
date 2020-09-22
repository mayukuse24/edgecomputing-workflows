import gzip
import json

from flask import Flask, request, jsonify
import docker

from workflow_handler import WorkflowHandler

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Welcome to the workflow manager'

@app.route('/workflow/surveil/', methods=['POST'])
def compress():
    content = request.json

    try:
        data = content["data"]
    except KeyError:
        return jsonify({"error": "data to compress not passed in input"})


    # TODO: execute workflow. Do this async?
    # WorkflowHandler.run_workflow_a(swarm_client, input_data, persist=True/False)

    json_data = json.dumps(data).encode('utf-8')

    compressed_data = gzip.compress(json_data)

    return jsonify({"data_bytes": str(compressed_data)})

if __name__ == "__main__": 
    app.run(host ='0.0.0.0', port = 5000, debug = True)

    # TODO: connect to swarm
    client = docker.from_env()
    
    # TODO: create workflow manager instance