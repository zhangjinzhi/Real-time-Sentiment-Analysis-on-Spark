from flask import Flask
from flask import request
from flask import abort
import flask
import json
import os
from flask import Response
from model import getWords

app = Flask(__name__)


@app.route('/check', methods=['POST'])
def check():
    input_txt = request.json
    a = input_txt['input_txt']
    b = getWords(a)
    return flask.jsonify(b)
   # return flask.jsonify(input_txt['input_txt'])


@app.route('/jquery-3.3.1.min.js', methods=['get'])
def jquery():
    html = open("jquery-3.3.1.min.js").read()
    return html


@app.route('/', methods=['get'])
def index():
    html = open("index.html").read()
    return html


@app.route('/<path>')
def today(path):
    base_dir = os.path.dirname(__file__)
    resp =Response(open(os.path.join(base_dir, path)).read())
    resp.headers["Content-type"]="text/plan;charset=UTF-8"
    return resp

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)

