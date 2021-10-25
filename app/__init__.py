import os
from flask import Flask
from flask_cors import CORS
from pymongo import MongoClient
from yaml import load, Loader

app = Flask(__name__, instance_relative_config=True)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config.from_mapping(
    SECRET_KEY='dev',
    DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
)

with open('./config.yaml', 'r') as f:
    config = dict(load(f, Loader=Loader))
    print(config["username"])

client = MongoClient("mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                     (config["username"], config["pw"], config["mongodb_uri"]), connect=False)
db = client["MetaSynDB"]
all_cards = db.AllCards


# ensure instance folder exists
try:
    os.makedirs(app.instance_path)
except OSError:
    pass

# Keep this import here to avoid circular imports
from app import routes
