import json
import os

from multiprocessing import Process

import pandas as pd
import redis

from flask import Flask, jsonify, request
from flask_cors import CORS

from utils.common_utils import dict_filter
from utils.dataset_utils import refresh_adawat_and_tags
from utils.gh_utils import create_issue


app = Flask(__name__)
app.config.from_object('config.Config')
CORS(app)


db = redis.from_url(app.config['REDIS_URL'])


@app.route('/datasets/schema')
def datasets_schema():
    adawat = json.loads(db.get('adawat'))

    return jsonify(list(adawat[0].keys()))


@app.route('/datasets')
def get_datasets():
    adawat = json.loads(db.get('adawat'))

    page = request.args.get('page', default=1, type=int)
    size = request.args.get('size', default=len(adawat), type=int)
    features = list(filter(None, request.args.get('features', default='', type=str).split(',')))
    query = request.args.get('query', default='', type=str)

    adawat_page = adawat[(page - 1) * size : page * size]

    if not adawat_page:
        return jsonify('Page not found.'), 404

    adawat_page = pd.DataFrame(adawat_page)

    if query:
        adawat_page = adawat_page.query(query)

    if features:
        adawat_page = adawat_page[features]

    return jsonify(adawat_page.to_dict('records'))


@app.route('/datasets/<int:index>')
def get_dataset(index: int):
    adawat = json.loads(db.get('adawat'))

    features = list(filter(None, request.args.get('features', default='', type=str).split(',')))

    if not (1 <= index <= len(adawat)):
        return jsonify(f'Dataset index is out of range, the index should be between 1 and {len(adawat)}.'), 404

    return jsonify(dict_filter(adawat[index - 1], features))


@app.route('/datasets/tags')
def get_tags():
    tags = json.loads(db.get('tags'))

    features = list(filter(None, request.args.get('features', default='', type=str).split(',')))

    return jsonify(dict_filter(tags, features))


@app.route('/datasets/<int:index>/issues', methods=['POST'])
def create_dataset_issue(index: int):
    adawat = json.loads(db.get('adawat'))

    if not (1 <= index <= len(adawat)):
        return jsonify(f'Dataset index is out of range, the index should be between 1 and {len(adawat)}.'), 404

    title = request.get_json().get('title', '')
    body = request.get_json().get('body', '')

    return jsonify({'issue_url': create_issue(f"{adawat[index]['Name']}: {title}", body)})


@app.route('/highlights')
def get_highlights():
    return jsonify({'highlights': "adawat is cool   "})


@app.route('/refresh/<string:password>')
def refresh(password: str):
    print('Refreshing globals...')

    if password != app.config['REFRESH_PASSWORD']:
        return jsonify('Password is incorrect.'), 403

    Process(name='refresh_globals', target=refresh_adawat_and_tags, args=(db,)).start()

    return jsonify('Datasets refresh process initiated successfully!')


with app.app_context():
    refresh(app.config['REFRESH_PASSWORD'])
