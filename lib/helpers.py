import os
import json
from sqlalchemy import create_engine
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

import pdb
"""
helpers.py

- a colleciton of functions that are shared between modules

API:
function db_connection              - connect to database

"""

def db_connect():
    # db_info should be kept in git ignore
    # an example file is kept in the repo for reference
    secrets = json.loads(open(os.path.join(__location__, 'secrets.json')).read())
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:5432/{}'.
                format(secrets['username'], secrets['password'], 
                       secrets['host'], secrets['db']))
    return engine