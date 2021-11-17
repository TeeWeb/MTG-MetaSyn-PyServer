# Tools for polling card data sources and updating the MongoDB as needed
import os
import requests
import json
import logging.config

from datetime import datetime
from typing import Collection, Dict, List
from pymongo import MongoClient
from itertools import chain


class Update():
    # Logger setup
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('db_updater')

    def __init__(self):
        self.update_cards = (self, 'cards')
        self.update(self, 'keywords')
        self.update(self, 'sets')
        self.update(self, 'types')

    @staticmethod
    def update(self, collection_name: str):
        self.logger.info('Running update on %s Collection' % (collection_name))
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
        self.logger.info('Requesting =>', url)
        try:
            r = requests.get(url, stream=True)
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
            with open(save_path, 'r') as data:
                latest_data = json.loads(data.read())
                return latest_data['meta']['date']
        except Exception as e:
            self.logger.error(e, exc_info=True)

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
        except Exception as e:
            cls.logger.critical("Invalid config path provided:",
                                new_config_path,
                                e,
                                exc_info=True)
        return

    def get_date_from_local_data(self, path_to_curr_data: str):
        date = None
        try:
            with open(path_to_curr_data, 'r') as f:
                data = json.loads(f.read())
                date = data['meta']['date']
        except Exception as e:
            self.logger.warning(
                "Unable to retrieve data from local data. Creating empty data file:",
                e)
            with open(path_to_curr_data, 'w') as f:
                f.write("{}")
        return date

    def local_update_needed(self, path_to_curr_data: str, data_endpoint: str):
        # Check date of current json data, if a local copy is available
        self.logger.info("Checking if local update needed")
        local_data_date = self.get_date_from_local_data(self,
                                                        path_to_curr_data=str)
        # Sets up a file to temporarily store latest data while comparing dates
        save_dest = data_endpoint.split("/").pop()
        latest_data_date = self.get_data(
            self, data_endpoint, self.data_dir_path + "new" + str(save_dest))

        # Compare dates to determine if update is needed
        if local_data_date == latest_data_date:
            self.logger.info(
                "No new %s data available. Local data up-to-date as of %s" %
                (save_dest, local_data_date))
            return False
        else:
            self.logger.info(
                "New %s data available (local file out-of-date). Updating local file with data from %s"
                % (save_dest, latest_data_date))
            return True

    @classmethod
    def get_db_collection(cls, collection: str):
        cls.logger.info("%s connecting to %s DB Collection at %s" %
                        (cls.get_db_user, collection, cls.get_db_url))
        try:
            client = MongoClient(
                "mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                (cls.get_db_user(), cls.config['pw'], cls.get_db_url()))
        except ConnectionError as e:
            cls.logger.critical("Unable to connect to DB", e, exc_info=True)
            return
        db = client['MetaSynDB']
        switch = {
            "keywords": db['keywords'],
            "types": db['types'],
            "sets": db['sets'],
            "cards": db['cards']
        }
        try:
            db_collection = switch.get(collection,
                                       lambda: "Invalid collection specified")
        except Exception as e:
            cls.logger.critical("Unable to get %s Collection" % (collection))
        return db_collection

    def get_new_items(self, collection: Collection, latest_data: Dict,
                      data_key: str):
        new_items = []
        # Specifies which key to use for item in MongoDB Collection
        collection_key = {
            "keywords": "keyword",
            "sets": "code",
            "cards": "identifiers",
            "identifiers": "scryfallOracleId",
            "types": "type"
        }

        # Check each item from latest data pull against those in the DB Collection
        # Return list of new items that are not already in the DB Collection
        self.logging.info("Checking DB for new %s..." % (data_key))
        start = datetime.now()
        # Check the cards collection requires an additional step, so check data_key first
        if data_key == "cards":
            for item in latest_data[data_key]:
                if collection.count_documents(
                    {collection_key[data_key]: {
                         "scryfallOracleId": item
                     }}) == 0:
                    self.logging.info(
                        "%s is not currently in DB (looked for { %s: { %s }})"
                        % (item, collection_key[data_key],
                           collection_key[collection_key[data_key]]))
                    new_item = {collection_key[collection_key[data_key]]: item}
                    new_items.append(new_item)
        else:
            for item in latest_data[data_key]:
                if collection.count_documents({collection_key[data_key]:
                                               item}) == 0:
                    self.logging.info(
                        "%s is not currently in DB (looked for { %s })" %
                        (item, collection_key[data_key]))
                    new_item = {collection_key[data_key]: item}
                    new_items.append(new_item)
        end = datetime.now()
        self.logging.info("Total Time to check DB:", end - start)
        return new_items

    def insert_new_items(self, collection: Collection, new_items: List):
        self.logging.warning("Inserting %s new items into DB..." %
                             (len(new_items)))
        start = datetime.now()
        db_results = {}
        ### Comment out the next 3 lines when testing to avoid writing to DB
        try:
            db_results = collection.insert_many(new_items)
            self.logging.warning("Added new items to DB:", db_results)
        except Exception as e:
            db_results = e
            self.logging.error("Exception while inserting item to DB:", e)

        ### Use below print() statement for testing to avoid writing to DB
        # print("(TEST) Adding new items to DB: %s" % (str(new_items)))
        end = datetime.now()
        self.logging.info("Total Time to insert items into DB:", end - start)
        return db_results

    # TODO: create function to retrieve and update AllCards collection in DB
    @classmethod
    def handle_cards_update(cls):
        cls.logging.info("--- Cards ---")
        # Check release date of current card data
        if cls.local_update_needed(
                cls, cls.data_dir_path + 'AtomicCards.json',
                'https://mtgjson.com/api/v5/AtomicCards.json'):
            # Get latest sets from newAtomicCards.json
            # TODO: Use 'newAtomicCards.json' that has already been featched rather than requesting again
            new_data = requests.get(
                'https://mtgjson.com/api/v5/AtomicCards.json').json()
            last_data_update = new_data['meta']['date']
            cards = []
            oracle_ids = {"cards": []}
            # AtomicCards data contains objects where:
            # KEY is a card name (key=<card_name>)
            # VALUE is an array of objects representing versions of cards with that name
            # Build a list of card versions and create AtomicCards.json file
            for card_name in new_data['data']:
                for card_version in new_data['data'][card_name]:
                    # Create a hash of card's faceName + scryfallOracleId to create a unique "_id" value
                    try:
                        card_version['_id'] = hash(
                            (card_version['identifiers']['scryfallOracleId'],
                             card_version['faceName']))
                    except:
                        card_version['_id'] = hash(
                            card_version['identifiers']['scryfallOracleId'])
                    # Remove foreignData and printings from cardData to reduce size
                    try:
                        del card_version['foreignData']
                    except Exception as e:
                        cls.logging.warning("Exception:", e)
                    try:
                        del card_version['printings']
                    except Exception as e:
                        cls.logging.warning("Exception:", e)
                    cards.append(card_version)
                    oracle_ids['cards'].append(
                        str(card_version['identifiers']['scryfallOracleId']))
            cards_data = {"meta": {"date": last_data_update}, "cards": cards}
            with open(cls.data_dir_path + 'AtomicCards.json', 'w') as f:
                f.write(json.dumps(cards_data, indent=4))
            # Compare most recent list of cards with DB Collection
            collection = cls.get_db_collection('cards')
            # TODO: add function to run synergy calculator on new cards BEFORE they're inserted into database
            # Get a list of dicts{'scryfallOracleId': <id>} that are not already in the DB Collection
            new_oracle_ids = cls.get_new_items(cls, collection, oracle_ids,
                                               'cards')
            if len(new_oracle_ids) > 0:
                # Find the corresponding card object for each scryfallOracleId in new_oracle_ids
                new_items = []
                cls.logging.info(new_oracle_ids)
                new_ids = []
                for oid in new_oracle_ids:
                    new_ids.append(oid['scryfallOracleId'])
                for card in cards_data['cards']:
                    if card['identifiers']['scryfallOracleId'] in new_ids:
                        # Once set object is found within cards_data, add it to the list of items to insert into DB
                        new_items.append(card)
                        print("Prepping %s to add to DB: %s" %
                              (card['identifiers']['scryfallOracleId'],
                               card['_id']))
                # Insert all new set objects into the DB
                cls.insert_new_items(cls, collection, new_items)
            else:
                cls.logging.info("No new cards")
        # Remove temp newSets.json file now that sets.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newAtomicCards.json')
        except Exception as e:
            cls.logging.error("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    @classmethod
    def handle_sets_update(cls):
        cls.logging.info("--- Sets ---")
        # Check release date of latest data for any updates
        if cls.local_update_needed(cls, cls.data_dir_path + 'SetsList.json',
                                   'https://mtgjson.com/api/v5/SetList.json'):
            # Get latest sets from newSets.json
            # TODO: Use 'newSetList.json' that has already been featched rather than requesting again
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
            with open(cls.data_dir_path + 'SetsList.json', 'w') as f:
                f.write(json.dumps(sets_data, indent=4))
            # Compare most recent list of sets with DB Collection
            collection = cls.get_db_collection('sets')
            # TODO: add function to run synergy calculator on new sets BEFORE they're inserted into database
            # Get a list of dicts{'code': <set_code>} that are not already in the DB Collection
            new_set_codes = cls.get_new_items(cls, collection, set_codes,
                                              'sets')
            # Find the corresponding sets_data object for each code in new_set_codes
            new_items = []
            for h in new_set_codes:
                for i in sets_data['sets']:
                    if i['code'] == h['code']:
                        # Once set object is found within sets_data, add it to the list of items to insert into DB
                        new_items.append(i)
                        cls.logging.info(
                            "Prepping set object for DB insertion:", i)
            # Insert all new set objects into DB
            cls.insert_new_items(cls, collection, new_items)
        # Remove temp newSets.json file now that sets.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newSetList.json')
        except Exception as e:
            cls.logging.error("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    @classmethod
    def handle_types_update(cls):
        cls.logging.info("--- Types ---")
        # Check release date of latest data for any updates
        if cls.local_update_needed(
                cls, cls.data_dir_path + 'CardTypes.json',
                'https://mtgjson.com/api/v5/CardTypes.json'):
            # Get latest card types
            # TODO: Use 'newCardTypes.json' that has already been featched rather than requesting again
            raw_data = requests.get(
                'https://mtgjson.com/api/v5/CardTypes.json').json()
            types_data = {
                "meta": {
                    "date": raw_data['meta']['date']
                },
                "types": {}
            }
            for card_type in raw_data['data']:
                subtypes = []
                for subtype in raw_data['data'][card_type]['subTypes']:
                    subtypes.append(subtype)
                types_data['types'][card_type] = subtypes
            with open(cls.data_dir_path + '/CardTypes.json', 'w') as f:
                f.write(json.dumps(types_data, indent=4))
            collection = cls.get_db_collection('types')
            # Compare most recent list of types with DB Collection
            # Insert any new types that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new types BEFORE they're inserted into database
            new_items = cls.get_new_items(cls, collection, types_data, 'types')
            for new_item in new_items:
                new_item["subtypes"] = types_data['types'][new_item['type']]
            cls.insert_new_items(cls, collection, new_items)
        # Remove temp newCardTypes.json file now that CardTypes.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newCardTypes.json')
        except Exception as e:
            cls.logging.error("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    @classmethod
    def handle_keywords_update(cls):
        # Check release date of latest data for any updates
        cls.logging.info("--- Keywords ---")
        if cls.local_update_needed(cls, cls.data_dir_path + 'Keywords.json',
                                   'https://mtgjson.com/api/v5/Keywords.json'):
            # Get latest keywords from newKeywords.json
            # TODO: Use 'newKeywords.json' that has already been featched rather than requesting again
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
            collection = cls.get_db_collection('keywords')
            # Insert any new keywords that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new keywords BEFORE they're inserted into database
            new_items = cls.get_new_items(cls, collection, keyword_data,
                                          'keywords')
            cls.insert_new_items(cls, collection, new_items)
        # Remove temp newKeywords.json file now that Keywords.json file has been updated
        try:
            os.remove(cls.data_dir_path + 'newKeywords.json')
        except Exception as e:
            cls.logging.error("Unable to remove temp data file",
                              e,
                              exc_info=True)
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
    updater = DBUpdater()
