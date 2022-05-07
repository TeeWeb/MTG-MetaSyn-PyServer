# Tools for polling card data sources and updating the MongoDB as needed
from abc import ABC, abstractstaticmethod
import chunk
import os
import re
from types import NoneType
from urllib.request import Request
import requests
import json
from datetime import datetime
from typing import Collection, Dict, List

from app.mtg_collections.local_data import ILocalData

###
# Abstract class for implementing Updater objects for specific data sets (i.e. Cards, Types, Keywords, Sets)
###
class IUpdater(ABC):
    data_dir_path = './app/data/'
    _capitalized_name = None
    _collection_name = None
    _data_endpoint = None

    def __init__(self):
        # DB
        self.get_db_collection = self.__get_db_collection
        # Local Data
        self.local_data = ILocalData(self.data_dir_path, self.get_title(), self.get_local_file_name())
        self.get_date = self.__get_date_last_updated
        self.get_latest_api_data = self.__request_data
        self.get_cached_date = self.__get_cached_data_date
        self.is_outdated = self.__check_for_new_data
        self.get_data_from_api = self.__request_data

    ###
    # Private methods
    ###
    def __str__(self):
        return "\n-- {} Updater --\nData Endpoint: {}\nLast Updated: {}\nIs Outdated? {}".format(self.get_title(), self.get_data_endpoint(), self.get_date(), self.is_outdated())

    def __request_data(self) -> requests.Response:
        print('Requesting =>', self.get_data_endpoint())
        try:
            r = requests.get(self.get_data_endpoint(), stream=True)
            return r
        except Exception as e:
            print(e)

    def __get_db_collection(self):
        try:
            db_collection = self.get_db_client[self.get_collection_name()]
        except Exception as e:
            print("Unable to get %s Collection: " % (self.get_collection_name(), e))
        return db_collection

    def __filter_for_new_data(self, collection, sets_dict: dict):
        # Get a list of dicts{'code': <set_code>} that are not already in the DB Collection
        new_set_codes = self.get_new_items(self, collection, sets_dict['sets'], 'sets')
        # Find the corresponding sets_data object for each code in new_set_codes
        new_items = []
        for h in new_set_codes:
            for i in sets_dict['sets']:
                if i['code'] == h['code']:
                    # Once set object is found within sets_data, add it to the list of items to insert into DB
                    new_items.append(i)
                    print("Prepping set object for DB insertion:", i)
        return new_items

    def __get_date_last_updated(self) -> str:
        return self.local_data.get_date()

    def __get_cached_data_date(self) -> str:
        return self.local_data.get_temp_data_date()

    def __check_for_new_data(self) -> bool:
        print("Checking for new '" + self.get_collection_name() + "' data...")
        last_updated = self.__get_date_last_updated()
        cached = self.__get_cached_data_date()
        if type(last_updated) == NoneType:
            print("No local data saved. Saving latest data to new " + self.get_local_file_name() + " file")
            with open(self.local_data.get_file_path(), 'w') as f:
                f.write(json.dumps({}))
            self.update_local_data()
            last_updated = self.__get_cached_data_date()
        if type(cached) == NoneType:
            print("No date from Cached Data. Pulling new latest API data.")
            cached = self.cache_data_from_api()
        if str(last_updated) < str(cached):
            print("Cached data is newer than last update. Last Updated: %s Cached: %s" % (last_updated, cached))
            return True
        print("No updates available. Last Updated: %s Cached: %s" % (last_updated, cached))
        return False

    ###
    # Abstract class methods
    ###
    @abstractstaticmethod
    def cache_data_from_api(self) -> str:
        pass

    @abstractstaticmethod
    def update_local_data(self):
        pass

    ###
    # Getters
    ###
    def get_local_file_name(self) -> str:
        return self.get_data_endpoint().split("/").pop()

    def get_local_file_path(self) -> str:
        return self.data_dir_path + self.get_local_file_name()

    def get_collection_name(self) -> str:
        return self._collection_name

    def get_title(self) -> str:
        return self._capitalized_name

    def get_data_endpoint(self) -> str:
        return self._data_endpoint

    ###
    # Utility Methods
    ###
    @classmethod
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
        print("Checking DB for new %s..." % (data_key))
        start = datetime.now()
        # Check the cards collection requires an additional step, so check data_key first
        if data_key == "cards":
            for item in latest_data[data_key]:
                if collection.count_documents(
                    {collection_key[data_key]: {
                         "scryfallOracleId": item
                     }}) == 0:
                    print(
                        "%s is not currently in DB (looked for { %s: { %s }})"
                        % (item, collection_key[data_key],
                           collection_key[collection_key[data_key]]))
                    new_item = {collection_key[collection_key[data_key]]: item}
                    new_items.append(new_item)
        else:
            for item in latest_data[data_key]:
                if collection.count_documents({collection_key[data_key]:
                                               item}) == 0:
                    print(
                        "%s is not currently in DB (looked for { %s })" %
                        (item, collection_key[data_key]))
                    new_item = {collection_key[data_key]: item}
                    new_items.append(new_item)
        end = datetime.now()
        print("Total Time to check DB:", end - start)
        return new_items

    @classmethod
    def insert_new_items(self, collection: Collection, new_items: List):
        print("Inserting %s new items into DB..." %
                             (len(new_items)))
        start = datetime.now()
        db_results = {}
        ### Comment out the next 3 lines when testing to avoid writing to DB
        try:
            db_results = collection.insert_many(new_items)
            print("Added new items to DB:", db_results)
        except Exception as e:
            db_results = e
            print("Exception while inserting item to DB:", e)

        ### Use below print() statement for testing to avoid writing to DB
        # print("(TEST) Adding new items to DB: %s" % (str(new_items)))
        end = datetime.now()
        print("Total Time to insert items into DB:", end - start)
        return db_results

    @classmethod
    def update(self, collection_name: str):
        print('Running update on %s Collection' % (collection_name))
        print("Starting " + collection_name + " update")
        switch = {
            "keywords": self.handle_keywords_update,
            "types": self.handle_types_update,
            "sets": self.handle_sets_update,
            "cards": self.handle_cards_update
        }
        update = switch.get(collection_name,
                            lambda: "Invalid collection specified")
        print("Finishing " + collection_name + " update")
        return
