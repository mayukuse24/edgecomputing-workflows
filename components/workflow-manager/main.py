import gzip
import json
import requests

from flask import Flask, request, jsonify
import docker

#from workflow_handler import WorkflowHandler
from workflow_handlerB import WorkflowHandler

app = Flask(__name__)

workflow_handler = None
wh =None

@app.route('/')
def hello_world():
    return 'Welcome to the workflow manager'
@app.route('/test',methods=['POST'])
def test_starting():

    a = wh.gen_flow_init_test()
    wh.start_generic_test(a)

    return 'Welcome to the workflow manager'


@app.route('/data', methods=['POST'])
def test_():
    print(request.get_json(force=True))
    wh.gen_dataflow_test(request.get_json(force=True),0)

    return 'Welcome to the workflow manager'
"""
@app.route('/workflow/surveil', methods=['POST'])
def compress():
    global workflow_handler

    is_persist = request.args.get('persist', False)

    try:
        data = request.files["audio"]
    except KeyError:
        return jsonify({"error": "audio file not provided"})

    # TODO: execute workflow. Do this async?
    if is_persist is False:
        result = workflow_handler.run_workflow_a_temp(data)
    else:
        result = workflow_handler.run_workflow_a_persist(data)

    return jsonify({"status": "ok"})
"""
if __name__ == "__main__":

    wh = WorkflowHandler()
    wh.gen_init_test()
    app.run(host ='0.0.0.0', port = 7002, debug = True)