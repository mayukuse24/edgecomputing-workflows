import gzip
import json
import requests
import threading
import random
import uuid
import timeit

from flask import Flask, request, jsonify
import docker

#from workflow_handler import WorkflowHandler
#from workflow_handlerB import WorkflowHandler
from workflow_handlerC import WorkflowHandler

app = Flask(__name__)

workflow_handler = None
wh =None
unrouted_data = {}

@app.route('/')
def hello_world():
    return 'Welcome to the workflow manager'

@app.route('/workflow',methods=['POST'])
def create_workflow():
    req_body = request.get_json(force=True)

    dataflow = req_body["dataflow"]

    workflow_id = uuid.uuid4()

    is_persist = True if request.args.get('persist') else False

    workflow_handler.create_workflow(workflow_id, dataflow, is_persist)

    return jsonify({"status": "ok","id": workflow_id})

@app.route('/aggregate',methods=['POST'])
def aggregate():
    try:
        workflow_id = request.args.get('workflow_id')

        workflow_id = uuid.UUID(workflow_id)
    except:
        return jsonify({"status": "Invalid workflow id or none provided"})

    output = workflow_handler.aggregate(query, workflow_id)

    return json.dumps({"summary": output}, default=str)

@app.route('/data', methods=['POST'])
def data_received():
    try:
        workflow_id = request.args.get('workflow_id')

        workflow_id = uuid.UUID(workflow_id)
    except:
        return jsonify({"status": "Invalid workflow id or none provided"})

    try:
        data = request.files["audio"]
    except KeyError:
        return jsonify({"status": "Error audio stream not provided"})

    start = timeit.default_timer()

    resp = workflow_handler.run_dataflow(workflow_id, data)

    stop = timeit.default_timer()

    print("dataflow_time:{} {} secs".format(workflow_id, stop - start))

    # Using json.dumps to handle ObjectId type fields by converting to str
    return json.dumps({"status": "ok", "response": resp}, default=str)

'''
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
'''
 
if __name__ == "__main__":
    workflow_handler = WorkflowHandler()
    #work.gen_init_test()

    app.run(host ='0.0.0.0', port = 7003, debug = False)