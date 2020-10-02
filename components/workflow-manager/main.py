import gzip
import json
import requests

from flask import Flask, request, jsonify
import docker

from workflow_handler import WorkflowHandler

app = Flask(__name__)

workflow_handler = None

@app.route('/')
def hello_world():
    return 'Welcome to the workflow manager'

@app.route('/workflow/surveil', methods=['POST'])
def compress():
    global workflow_handler

    content = request.json

    try:
        data = content["data"]
    except KeyError:
        return jsonify({"error": "data point not provided"})

    # TODO: execute workflow. Do this async?
    result = workflow_handler.run_workflow_a_temp(data)

    # json_data = json.dumps(result).encode('utf-8')

    return jsonify({"response": result})

if __name__ == "__main__": 
    workflow_handler = WorkflowHandler()

    app.run(host ='0.0.0.0', port = 7000, debug = True)

    # TODO: create workflow manager instance
    # workflowHandler = WorkflowHandler()