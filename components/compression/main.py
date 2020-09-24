import gzip
import json

from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Welcome to the python compress component'

@app.route('/compress', methods=['POST'])
def compress():
    content = request.json

    try:
        data = content["data"]
    except KeyError:
        return jsonify({"error": "data to compress not passed in input"})

    json_data = json.dumps(data).encode('utf-8')

    compressed_data = gzip.compress(json_data)

    return jsonify({"data_bytes": str(compressed_data)})

if __name__ == "__main__": 
    app.run(host ='0.0.0.0', port = 5001, debug = True)