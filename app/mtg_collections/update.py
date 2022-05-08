# Tools for polling card data sources and updating the MongoDB as needed
from abc import ABC, abstractstaticmethod
from types import NoneType
import requests
import json
from datetime import datetime
from typing import Collection, Dict, List

from app.mtg_collections.local_data import ILocalData
from app.mtg_collections.api_data import IAPIData

###
# Abstract class for implementing Updater objects for specific data sets (i.e. Cards, Types, Keywords, Sets)
###
class IUpdater(ABC):
    _capitalized_name = None
    _collection_name = None
    _data_endpoint = None
    _identifier = None

    def __init__(self, db_collection: Collection):
        # Mongo DB Collection
        self.collection = db_collection
        # Local Data
        self.local = ILocalData(self._collection_name, self.get_local_file_name())
        # API Data
        self.api = IAPIData(self._data_endpoint)

    @abstractstaticmethod
    def get_items_to_add(self) -> list:
        pass

    @abstractstaticmethod
    def get_items_to_update(self) -> list:
        pass

    ###
    # Private methods
    ###
    def __str__(self) -> str:
        return "\n-- {} Updater --\nData Endpoint: {}\nLast Updated: {}\nIs Outdated? {}".format(self.get_title(), self.get_data_endpoint(), self.local.get_date(), self.is_outdated())

    def __filter_for_new_data(self, collection, sets_dict: dict):
        # Get a list of dicts{'code': <set_code>} that are not already in the DB Collection
        new_set_codes = self.get_items_to_add(self, collection, sets_dict['sets'], 'sets')
        # Find the corresponding sets_data object for each code in new_set_codes
        new_items = []
        for h in new_set_codes:
            for i in sets_dict['sets']:
                if i['code'] == h['code']:
                    # Once set object is found within sets_data, add it to the list of items to insert into DB
                    new_items.append(i)
                    print("Prepping set object for DB insertion:", i)
        return new_items

    ###
    # Getters
    ###
    def get_local_file_name(self) -> str:
        return self.get_data_endpoint().split("/").pop()

    def get_local_file_path(self) -> str:
        return self._data_dir_path + self.get_local_file_name()

    def get_collection_name(self) -> str:
        return self.collection._Collection__name

    def get_title(self) -> str:
        return self._capitalized_name

    def get_data_endpoint(self) -> str:
        return self._data_endpoint

    ###
    # Utility Methods
    ###
    def is_outdated(self) -> bool:
        print("\nChecking for new '" + self.get_collection_name() + "' data...")
        last_updated = self.local.get_date()
        if type(last_updated) == FileNotFoundError:
            print("No local data saved. Fetching latest data for cache.")
            self.fetch_latest_api_data()
            last_updated = self.local.get_temp_data_date()
            self.local.update()
            return True
        cached = self.local.get_temp_data_date()
        if type(cached) == FileNotFoundError:
            print("No date from Cached Data. Pulling new latest API data.")
            cached = self.api.get()
        if str(last_updated) < str(cached):
            print("Cached data is newer than last update. Last Updated: %s Cached: %s" % (last_updated, cached))
            return True

        print("No updates available. Last Updated: %s Cached: %s" % (last_updated, cached))
        return False

    # GET data from API, reformat (if needed), and store in temp data file (w/ "new" prefix) 
    def fetch_latest_api_data(self):
        raw_data = self.api.get()
        self.local.save_data_locally(raw_data)

    def sync(self):
        start = datetime.now()
        print("\n-- Syncing %s --" % self._capitalized_name)
        # Handle new items
        self.get_items_to_add()
        print("New %s items to add (%s): %s" % (self._capitalized_name, self.new_items.__len__(), self.new_items))
        # Handle updated items
        self.get_items_to_update()
        print("%s items to update (%s): %s" % (self._capitalized_name, self.new_attributes.__len__(), self.new_attributes))
        end = datetime.now()
        print("Total Time to check DB:", end - start)


    def handle_new_data(self) -> list:
        # Check each item from latest data pull against those in the DB Collection
        # Return list of new items that are not already in the DB Collection
        print("Checking DB for new %s..." % (self.collection.name))
        start = datetime.now()
        # Check the cards collection requires an additional step, so check data_key first
        new_items = self.get_items_to_add()
        if new_items.__len__() > 0:
            print("New %s data available" % self.get_collection_name())
                # if collection.count_documents(
                #     {collection_key[data_key]: {
                #          "scryfallOracleId": item
                #      }}) == 0:
                #     print(
                #         "%s is not currently in DB (looked for { %s: { %s }})"
                #         % (item, collection_key[data_key],
                #            collection_key[collection_key[data_key]]))
                #     new_item = {collection_key[collection_key[data_key]]: item}
                #     new_items.append(new_item)
        # else:
        #     for item in latest_data[data_key]:
        #         if collection.count_documents({collection_key[data_key]:
        #                                        item}) == 0:
        #             print(
        #                 "%s is not currently in DB (looked for { %s })" %
        #                 (item, collection_key[data_key]))
        #             new_item = {collection_key[data_key]: item}
        #             new_items.append(new_item)
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
