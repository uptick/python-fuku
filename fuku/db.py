import json
import os


def get_rc_path():
    return os.path.expanduser('~/.fukurc')


def get_default_db():
    path = os.path.expanduser(os.path.join(get_rc_path(), 'db.json'))
    dir = os.path.dirname(path)
    try:
        os.makedirs(dir)
    except OSError:
        pass
    if os.path.exists(path):
        with open(path, 'r') as inf:
            db = json.load(inf)
    else:
        db = {}
    return db


def save_db(db):
    path = os.path.expanduser(os.path.join(get_rc_path(), 'db.json'))
    with open(path, 'w') as outf:
        json.dump(db, outf, indent=2)
