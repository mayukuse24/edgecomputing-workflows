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

    try:
        workflow_id = request.args.get('workflow_id')
    except KeyError:
        return jsonify({"error": "workflow id not provided"})

    try:
        data = request.files["audio"]
    except KeyError:
        return jsonify({"error": "audio file not provided"})

    # TODO: execute workflow. Do this async?
    if is_persist is False:
        result = workflow_handler.run_workflow_a_temp(data, workflow_id)
    else:
        result = workflow_handler.run_workflow_a_persist(data, workflow_id)

    # Using json.dumps to handle ObjectId type fields by converting to str
    return json.dumps({"status": "ok", "response": result}, default=str)

if __name__ == "__main__": 
    workflow_handler = WorkflowHandler()

    app.run(host ='0.0.0.0', port = 7003, debug = True)