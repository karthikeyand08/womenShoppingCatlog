# encoding: utf-8
import os
from flask import Flask, request
from RedisJsonHandler import RedisToJson
from datetime import datetime

app = Flask(__name__)

INVALID_REQ_PARAMS = 'Invalid request parameters'
INVALID_DATE_FORMAT = 'Given invalid data format. ' \
                      'Should be in YYYY-MM-DD format'

NO_DATA_AVAILABLE = 'No data available for {}'


def check_date_param_issue(_date):
    if not _date:
        return INVALID_REQ_PARAMS

    try:
        _test_data = datetime.strptime(_date, '%Y-%m-%d')
    except ValueError:
        return INVALID_DATE_FORMAT

    return None


@app.route('/')
def home():
    return """<b>Welcome to Women's shoes shopping REST api!!!</b>"""


@app.route('/getRecentItem')
def get_recent_item():
    _date = request.args.get('date')

    status = check_date_param_issue(_date)

    if status:
        return status

    with RedisToJson() as rtj:
        response = rtj.get_latest_for_date(_date)

    if not response:
        return NO_DATA_AVAILABLE.format(_date)

    return response


@app.route('/getBrandsCount')
def get_brand_count():
    _date = request.args.get('date')

    status = check_date_param_issue(_date)

    if status:
        return status

    with RedisToJson() as rtj:
        response = rtj.get_brand_count_by_date(_date)

    if not response:
        return NO_DATA_AVAILABLE.format(_date)

    return response


@app.route('/getItemsbyColor')
def get_items_by_color():
    _color = request.args.get('color')

    with RedisToJson() as rtj:
        response = rtj.get_items_by_color(_color)

    if not response:
        return NO_DATA_AVAILABLE.format(_color)

    return response


app.run(debug=True,
        host=os.environ.get('REST_SERVER_HOST', 'localhost'),
        port=os.environ.get('REST_SERVER_PORT', 5000))
