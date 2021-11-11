# Tools for polling card data sources and updating the MongoDB as needed
import os
from typing import Collection, Dict, List
import requests
import zipfile
from yaml import load, Loader
import argparse
import json
from mtgsdk import Card, Set, card
from pymongo import MongoClient
from itertools import chain


class DBUpdater():

    # MongoDB config access file
    config_path = '../../config.yaml'
    config = None
    with open(config_path, 'r') as f:
        config = dict(load(f, Loader=Loader))
    data_dir_path = '../data/'

    def __init__(self):
        self.update(self, 'keywords')
        self.update(self, 'sets')

    @staticmethod
    def update(self, collection_name: str):
        switch = {
            "keywords": self.handle_keywords_update,
            "types": self.handle_types_update,
            "sets": self.handle_sets_update,
            "cards": self.handle_cards_update
        }
        update = switch.get(collection_name,
                            lambda: "Invalid collection specified")
        update()
        return

    def get_data(self, url: str, save_path: str, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
        if ".zip" in save_path:
            with zipfile.ZipFile(save_path, 'r') as unzipped:
                unzipped.extractall(self.data_dir_path)
            with open(save_path.rstrip(".zip"), 'r') as data:
                latest_data = json.loads(data.read())
        else:
            with open(save_path, 'r') as data:
                latest_data = json.loads(data.read())
        return latest_data['meta']['date']

    @classmethod
    def get_db_user(cls):
        return cls.config['username']

    @classmethod
    def get_db_url(cls):
        return cls.config['mongodb_uri']

    @classmethod
    def set_config_path(cls, new_config_path: str):
        try:
            with open(new_config_path, 'r') as f:
                pass
            cls.config_path = new_config_path
        except:
            Exception("Invalid path provided:", new_config_path)
        return

    def get_date_from_local_data(path_to_curr_data):
        date = None
        try:
            with open(path_to_curr_data, 'r') as f:
                data = json.loads(f.read())
                date = data['meta']['date']
        except Exception as e:
            print("!!! get_date_from_local_data() - ", e)
        return date

    def local_update_needed(self, path_to_curr_data: str, data_endpoint: str):
        # Check date of current Keywords.json data, if a local copy is available
        try:
            with open(path_to_curr_data, "r") as card_data:
                current_data = json.loads(card_data.read())
                local_data_date = current_data['meta']['date']
        # Failed to find local data file or read the date. Creates an empty json file.
        except Exception as e:
            print(e)
            with open(path_to_curr_data, 'w') as f:
                f.write("{}")
            local_data_date = ""

        # Sets up a file to temporarily store latest data while comparing dates
        save_dest = data_endpoint.split("/").pop()
        latest_data_date = self.get_data(
            self, data_endpoint, self.data_dir_path + "new" + str(save_dest))

        # Compare dates to determine if update is needed
        if local_data_date == latest_data_date:
            print("No new %s data available. Local data up-to-date as of %s" %
                  (save_dest, local_data_date))
            return False
        else:
            print(
                "New %s data available (local file out-of-date). Updating local file with data from %s"
                % (save_dest, latest_data_date))
            return True

    @staticmethod
    def get_db_collection(user: str, pw: str, url: str, collection: str):
        try:
            client = MongoClient(
                "mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                (user, pw, url))
        except ConnectionError:
            print("Unable to connect to DB")
            return
        db = client['MetaSynDB']
        switch = {
            "keywords": db['keywords'],
            "types": db['types'],
            "sets": db['sets'],
            "cards": db['AllCards']
        }
        db_collection = switch.get(collection,
                                   lambda: "Invalid collection specified")
        return db_collection

    @staticmethod
    def get_new_items(collection: Collection, latest_data: Dict,
                      data_key: str):
        new_items = []
        # Specifies which key to use for item in MongoDB Collection
        collection_key = {"keywords": "keyword", "sets": "code"}

        # Check each item from latest data pull against those in the DB Collection
        # Return list of new items that are not already in the DB Collection
        for item in latest_data[data_key]:
            print("Checking DB for", item)
            if collection.count_documents({collection_key[data_key]:
                                           item}) == 0:
                new_item = {collection_key[data_key]: item}
                new_items.append(new_item)
        return new_items

    @staticmethod
    def insert_new_items(collection: Collection, new_items: List):
        inserted_ids = []
        for item in new_items:
            ### Comment out the next 3 lines when testing to avoid writing to DB
            new_id = collection.insert_one(item).inserted_id
            inserted_ids.append(new_id)
            print("Added new item to DB (%s): %s" % (str(new_id), str(item)))

            ### Use below print() statement for testing to avoid writing to DB
            # print("Added new item to DB: %s" % (str(item)))
        return inserted_ids

    # TODO: create function to retrieve and update AllCards collection in DB
    @classmethod
    def handle_cards_update(cls):
        # Check release date of current card data
        if cls.local_update_needed(
                './data/AtomicCards.json',
                'https://mtgjson.com/api/v5/AtomicCards.json.zip'):
            collection = cls.get_db_collection('cards')
            update_count = 0
            print("## Checking for new cards")
            with open('./data/AtomicCards.json', 'r') as card_data:
                current_data = json.loads(card_data.read())
                for card in current_data['data']:
                    if collection.count_documents(
                        {"multiverseId": card['multiverseId']}) == 0:
                        print("New Card Found ->", card['multiverseId'],
                              card['name'])
        else:
            print("### No updates to 'AllCards' DB collection needed")
            return

    @classmethod
    def handle_sets_update(cls):
        # Check release date of latest data for any updates
        if cls.local_update_needed(cls, cls.data_dir_path + 'sets.json',
                                   'https://mtgjson.com/api/v5/SetList.json'):
            # Get latest sets from newSets.json
            raw_data = requests.get(
                'https://mtgjson.com/api/v5/SetList.json').json()
            sets = []
            set_codes = {"sets": []}
            for card_set in raw_data['data']:
                sets.append(card_set)
                set_codes['sets'].append(card_set['code'])
            sets.sort(key=lambda set: set['code'])
            last_data_update = raw_data['meta']['date']
            sets_data = {"meta": {"date": last_data_update}, "sets": sets}
            with open(cls.data_dir_path + '/sets.json', 'w') as f:
                f.write(json.dumps(sets_data, indent=4))
            # Compare most recent list of sets with DB Collection
            collection = cls.get_db_collection(cls.get_db_user(),
                                               cls.config['pw'],
                                               cls.get_db_url(), 'sets')
            # TODO: add function to run synergy calculator on new sets BEFORE they're inserted into database
            # Get a list of dicts{'code': <set_code>} that are not already in the DB Collection
            new_set_codes = cls.get_new_items(collection, set_codes, 'sets')
            # Find the corresponding sets_data object for each code in new_set_codes
            new_items = []
            for h in new_set_codes:
                for i in sets_data['sets']:
                    if i['code'] == h['code']:
                        # Once set object is found within sets_data, add it to the list of items to insert into DB
                        new_items.append(i)
                        print("Prepping set object for DB insertion:", i)
            # Insert all new set objects into DB
            new_item_ids = cls.insert_new_items(collection, new_items)
            print("\nNew items added to DB: " + str(len(new_item_ids)),
                  new_item_ids)
        # Remove temp newSets.json file now that sets.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newSetList.json')
        except Exception as e:
            print(e)
        return

    @classmethod
    def handle_types_update(cls):
        # Get latest card types
        raw_data = requests.get(
            'https://mtgjson.com/api/v5/CardTypes.json').json()
        updated_types = {}
        for card_type in raw_data['data']:
            subtypes = []
            for subtype in raw_data['data'][card_type]['subTypes']:
                subtypes.append(subtype)
            updated_types[card_type] = subtypes
        with open(cls.data_dir_path + '/types.yaml', 'w') as f:
            f.write(str(updated_types))
        collection = cls.get_db_collection('types')
        # Compare most recent list of types with DB Collection
        # Insert any new types that are not already in the DB Collection
        # TODO: add function to run synergy calculator on new types BEFORE they're inserted into database
        update_count = 0
        print("## Checking for new Types and Subtypes")
        for card_type in updated_types:
            if collection.count_documents({"type": card_type}) == 0:
                new_card_type = dict(type=card_type,
                                     subtypes=updated_types[card_type])
                print(new_card_type)
                new_id = collection.insert_one(new_card_type).inserted_id
                update_count += 1
                print("Added new card_type to DB (" + str(new_id) + "): " +
                      card_type)
            elif collection.count_documents({"type": card_type}) == 1:
                for subtype in updated_types[card_type]:
                    if collection.count_documents({
                            "type": card_type,
                            "subtypes": subtype
                    }) == 0:
                        print("## FOUND NEW SUBTYPE: " + card_type + "-" +
                              subtype)
                        current_subtypes = collection.find({
                            "type": card_type
                        }, {
                            "_id": 0,
                            "subtypes": 1
                        }).next()['subtypes']
                        current_subtypes.append(subtype)
                        current_subtypes.sort()
                        print(current_subtypes)
                        collection.update(
                            {"type": card_type},
                            {"$set": {
                                "subtypes": current_subtypes
                            }})
                        print("Added new subtype to '" + card_type +
                              "' type in DB: " + subtype)
                        update_count += 1
        if update_count == 0:
            print("### No updates to 'types' DB collection needed")
            return
        else:
            print("### Number of new types added to DB: " + str(update_count))
            return

    @classmethod
    def handle_keywords_update(cls):
        # Check release date of latest data for any updates
        if cls.local_update_needed(cls, cls.data_dir_path + 'Keywords.json',
                                   'https://mtgjson.com/api/v5/Keywords.json'):
            # Get latest keywords from newKeywords.json
            new_data = requests.get(
                'https://mtgjson.com/api/v5/Keywords.json').json()
            sorted_keywords = cls.flatten_keywords_lists(new_data['data'])
            last_data_update = new_data['meta']['date']
            keyword_data = {
                "meta": {
                    "date": last_data_update
                },
                "keywords": sorted_keywords
            }
            with open(cls.data_dir_path + 'Keywords.json', 'w') as f:
                f.write(json.dumps(keyword_data, indent=4))
            # Compare most recent list of keywords with DB Collection
            collection = cls.get_db_collection(cls.get_db_user(),
                                               cls.config['pw'],
                                               cls.get_db_url(), 'keywords')
            # Insert any new keywords that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new keywords BEFORE they're inserted into database
            new_items = cls.get_new_items(collection, keyword_data, 'keywords')
            new_item_ids = cls.insert_new_items(collection, new_items)
            print("\nNew items added to DB: " + str(len(new_item_ids)),
                  new_item_ids)
        # Remove temp newKeywords.json file now that Keywords.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newKeywords.json')
        except Exception as e:
            print(e)
        return

    # Flatten lists of keywords data into one sorted list
    @staticmethod
    def flatten_keywords_lists(data: dict):
        flattened_iter = chain(data['abilityWords'], data['keywordAbilities'],
                               data['keywordActions'])
        keywords = []
        for i in flattened_iter:
            keywords.append(i)
        keywords.sort()
        sorted_keywords = dict.fromkeys(keywords)
        return sorted_keywords


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description=
    #     "Update app's MetaSyn database with latest data from MTGJSON.com. "
    #     "Default collection to update is AllCards")
    # parser.add_argument('-c',
    #                     '--collection',
    #                     type=str,
    #                     choices=['cards', 'types', 'keywords', 'sets'],
    #                     default="cards",
    #                     help="collection to update")

    # args = parser.parse_args()
    updater = DBUpdater()
