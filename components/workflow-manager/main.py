import gzip
import json
import requests
import threading
import random
import uuid

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

'''
@app.route('/flow',methods=['POST'])
def test_build():
    cont = request.get_json(force=True)
    a,id = wh.build_flow(cont["Components"])#wh.gen_flow_init_test_3()
    wh.start_generic_test(a)
    runner = threading.Thread(target=wh.run_workflow,args=(a.flow_id,))
    runner.start()
    return jsonify({"status": "ok","id":id})
'''

@app.route('/workflow',methods=['POST'])
def create_workflow():
    req_body = request.get_json(force=True)

    dataflow = req_body["dataflow"]

    workflow_id = uuid.uuid4()

    is_persist = True if request.args.get('persist') else False

    workflow_handler.create_workflow(workflow_id, dataflow, is_persist)

    return jsonify({"status": "ok","id": workflow_id})


'''
@app.route('/test_init',methods=['POST'])
def test_starting():

    a,id = wh.gen_flow_init_test_3()
    wh.start_generic_test(a)
    runner = threading.Thread(target=wh.run_workflow,args=(a.flow_id,))
    runner.start()
    return jsonify({"status": "ok","id":id})
'''

@app.route('/get_output',methods=['POST'])
def test_run_flow():

    output = wh.get_data(request.get_json(force=True)["workflow_id"])
    ret ="Current results of workflow id# "+str(request.get_json(force=True)["workflow_id"])+":"
    for out in output:
        ret = ret +", "+str(out)
    return  jsonify({"results:":ret})

'''
@app.route('/route', methods=['POST'])
def route_data():
    try:
        dat = unrouted_data[request.get_json(force=True)["location"]]
        workflow = request.get_json(force=True)["workflow_id"]
        print(dat[1])
        wh.gen_output(dat[0],workflow,dat[1])
    except:
        return jsonify({"status": "Failure to correctly route data"})
    return jsonify({"status": "sent to workflow"})

def get_location():
    return random.randint(0, 70000)
'''

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

    try:
        resp = workflow_handler.run_dataflow(workflow_id, data)
    except:
        return jsonify({"status": "failed to process data point"})

    return jsonify({"status": "data sent to workflow."})

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