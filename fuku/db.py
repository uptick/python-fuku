import os
import json
# from tinydb import TinyDB


def get_default_db():
    path = os.path.expanduser('~/.fuku.json')
    if os.path.exists(path):
        with open(path, 'r') as inf:
            db = json.load(inf)
    else:
        db = {}
    return db


def save_db(db):
    path = os.path.expanduser('~/.fuku.json')
    with open(path, 'w') as outf:
        json.dump(db, outf, indent=2)
