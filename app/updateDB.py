# Tools for polling card data sources and updating the MongoDB as needed

import requests
import zipfile
from yaml import load, Loader
import argparse
import json
from mtgsdk import Card, Set

from pymongo import MongoClient

parser = argparse.ArgumentParser(
    description="Update app's MetaSyn database with latest data from MTGJSON.com. Default collection to update is AllCards")
parser.add_argument('-c', '--collection', type=str, choices=[
                    'cards', 'types', 'keywords', 'sets', 'formats'], default="cards", help="collection to update")
args = parser.parse_args()

# MongoDB config access file
with open('../config.yaml', 'r') as f:
    config = dict(load(f, Loader=Loader))
    print("### MongoDB user: " + config['username'])


def get_data(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            f.write(chunk)
    with zipfile.ZipFile(save_path, 'r') as unzipped:
        unzipped.extractall('./server/metasyn_app/data/')

# db options include: "RawDataDB" and "MetaSynDB"


def update_db(collection_name):
    switch = {
        "keywords": handle_keywords_update,
        "types": handle_types_update
    }
    update = switch.get(
        collection_name, lambda: "Invalid collection specified")
    update()

# TODO: create function to retrieve and update AllCards collection in DB
# def handle_cards_update():
#     # Get latest cards
#     get_data("https://mtgjson.com/api/v5/AllPrintings.json.zip", "./server/flaskr/data/allPrintings.json.zip")


def handle_types_update():
    # Get latest card types
    raw_data = requests.get('https://mtgjson.com/api/v5/CardTypes.json').json()
    updated_types = {}
    for card_type in raw_data['data']:
        subtypes = []
        for subtype in raw_data['data'][card_type]['subTypes']:
            subtypes.append(subtype)
        updated_types[card_type] = subtypes
    with open('./data/types.yaml', 'w') as f:
        f.write(str(updated_types))
    # Poll MongoDB for current 'types' collection
    try:
        client = MongoClient("mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                             (config["username"], config["pw"], config["mongodb_uri"]))
    except ConnectionError:
        print("Unable to connect to DB")
        return
    db = client['MetaSynDB']
    collection = db['types']
    # Compare most recent list of types with DB Collection
    # Insert any new types that are not already in the DB Collection
    # TODO: add function to run synergy calculator on new types BEFORE they're inserted into database
    update_count = 0
    print("## Checking for new Card Types and subtypes")
    for card_type in updated_types:
        if collection.find({"type": card_type}).count() == 0:
            new_card_type = dict(
                type=card_type, subtypes=updated_types[card_type])
            print(new_card_type)
            new_id = collection.insert_one(new_card_type).inserted_id
            update_count += 1
            print("Added new card_type to DB (" + str(new_id) + "): " + card_type)
        elif collection.find({"type": card_type}).count() == 1:
            for subtype in updated_types[card_type]:
                if collection.find({"type": card_type, "subtypes": subtype}).count() == 0:
                    print("## FOUND NEW SUBTYPE: " + card_type + "-" + subtype)
                    current_subtypes = collection.find(
                        {"type": card_type}, {"_id": 0, "subtypes": 1}).next()['subtypes']
                    current_subtypes.append(subtype)
                    current_subtypes.sort()
                    print(current_subtypes)
                    collection.update({"type": card_type}, {
                                      "$set": {"subtypes": current_subtypes}})
                    print("Added new subtype to '" +
                          card_type + "' type in DB: " + subtype)
                    update_count += 1
    if update_count == 0:
        print("### No updates to card_types DB Collection needed")
        return
    else:
        print("### Number of new types added to DB: " + str(update_count))
        return


def handle_keywords_update():
    # Get latest keywords
    raw_data = requests.get('https://mtgjson.com/api/v5/Keywords.json').json()
    # Reformat keywords data into manageable list
    updated_keywords = []
    for data1 in raw_data['data']['abilityWords']:
        updated_keywords.append(data1)
    for data2 in raw_data['data']['keywordAbilities']:
        updated_keywords.append(data2)
    for data3 in raw_data['data']['keywordActions']:
        updated_keywords.append(data3)
    updated_keywords.sort()
    sorted_keywords = json.dumps(dict.fromkeys(updated_keywords))
    with open('./data/Keywords.yaml', 'w') as f:
        f.write(str(sorted_keywords))
    # Poll MongoDB for current 'keywords' collection
    try:
        client = MongoClient("mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                             (config["username"], config["pw"], config["mongodb_uri"]))
    except ConnectionError:
        print("Unable to connect to DB")
        return
    db = client['MetaSynDB']
    collection = db['keywords']
    # Compare most recent list of keywords with DB Collection
    # Insert any new keywords that are not already in the DB Collection
    # TODO: add function to run synergy calculator on new keywords BEFORE they're inserted into database
    update_count = 0
    for keyword in updated_keywords:
        if collection.find({"keyword": keyword}).count() == 0:
            new_keyword = dict(keyword=keyword)
            new_id = collection.insert_one(new_keyword).inserted_id
            update_count += 1
            print("Added new keyword to DB (" + str(new_id) + "): " + keyword)
    if update_count == 0:
        print("### No updates to keywords DB Collection needed")
        return
    else:
        print("### Number of new keywords added to DB: " + str(update_count))
        return


args = parser.parse_args()
update_db(args.collection)
