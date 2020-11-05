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
    
#     content = request.get_json(force=True)

    try:
        data = request.files["audio"]
        
#         components_data = content["components"]
#         filename = content["filename"]
#         components = components_data.keys()
#         component_flow = " "
#         for key, value in components_data.items():
#             flow = ' '.join(value)
#             #component_flow.join(str(key) + " : " + ' '.join(value))
#             complete_flow = key + " --> " + flow
#             component_flow += complete_flow + " "
#         return jsonify({"number": str(len(components)),
#                         "components": ' '.join(components),
#                         "data-flow": component_flow,
#                         "filename": str(filename)})
    except KeyError:
        return jsonify({"error": "audio file not provided"})

    # TODO: execute workflow. Do this async?
    if is_persist is False:
        result = workflow_handler.run_workflow_a_temp(data)
    else:
        result = workflow_handler.run_workflow_a_persist(data)

    return jsonify({"status": "ok"})

if __name__ == "__main__": 
    workflow_handler = WorkflowHandler()

    app.run(host ='0.0.0.0', port = 7002, debug = True)
