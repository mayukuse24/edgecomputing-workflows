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

    is_persist = request.args.get('persist', False)

    content = request.json

    try:
        data = content["data"]
    except KeyError:
        return jsonify({"error": "data point not provided"})

    # TODO: execute workflow. Do this async?
    if is_persist is False:
        result = workflow_handler.run_workflow_a_temp(data)
    else:
        result = workflow_handler.run_workflow_a_persist(data)

    return jsonify({"response": result})

if __name__ == "__main__": 
    workflow_handler = WorkflowHandler()

    app.run(host ='0.0.0.0', port = 7000, debug = True)

    # TODO: create workflow manager instance
    # workflowHandler = WorkflowHandler()