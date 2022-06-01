# Tools for polling card data sources and updating the MongoDB as needed
from abc import ABC, abstractstaticmethod
from datetime import datetime
from typing import Collection

from app.mtg_collections.local_data import ILocalData

###
# Abstract class for implementing Updater objects for specific data sets (i.e. Cards, Types, Keywords, Sets)
###
class IUpdater(ABC):
    _capitalized_name = None
    _data_endpoint = None
    _identifier = None

    def __init__(self, db_collection: Collection):
        # Mongo DB Collection
        self.collection = db_collection
        # Local Data
        self.local = ILocalData(self.get_collection_name(), self.get_local_file_name(), self._data_endpoint)

    @abstractstaticmethod
    def get_items_to_update(self) -> list:
        pass

    @abstractstaticmethod
    def get_items_to_add(self) -> list:
        pass

    @abstractstaticmethod
    def get_distinct_coll_items(self) -> list:
        pass

    ###
    # Private methods
    ###
    def __str__(self) -> str:
        return f"\n-- {self.get_title()} Updater --\nData Endpoint: {self.get_data_endpoint()}\nLast Updated: {self.local.get_date()}\nIs Outdated? {self.is_outdated()}"

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

    def __build_update_query(self) -> str:
        update_query = {}
        operator = "$or"
        stringified = str(update_query)
        print(">>> Update Query:", stringified)
        return stringified

    ###
    # Getters
    ###
    def get_local_file_name(self) -> str:
        return self.get_data_endpoint().split("/").pop()

    def get_collection_name(self) -> str:
        return self.collection._Collection__name

    def get_title(self) -> str:
        return self._capitalized_name

    def get_data_endpoint(self) -> str:
        return self._data_endpoint

    def get_whole_collection(self) -> Collection:
        return self.collection.find()

    def has_duplicates(self) -> bool:
        if self.get_distinct_coll_items().__len__() < self.collection.count_documents({}):
            print(self.get_distinct_coll_items().__len__())
            print(self.collection.count_documents({}))
            return True
        return False

    def print_date_last_updated(self) -> str:
        return self.local.get_date()

    ###
    # Utility Methods
    ###
    def sync(self):
        print("\n-- Syncing %s --" % self._capitalized_name)
        # Handle new items
        self.get_items_to_add()
        print(f"New {self._capitalized_name} items to add: {self.new_items.__len__()}")
        self.insert_new_items()

        # Handle updated items
        self.get_items_to_update()
        print(f"{self._capitalized_name} items to update: {self.new_attributes.__len__()}")
        self.update_items()


    # def handle_new_data(self) -> list:
    #     # Check each item from latest data pull against those in the DB Collection
    #     # Return list of new items that are not already in the DB Collection
    #     print(f"Checking DB for new {self.collection.name}...")
    #     start = datetime.now()
    #     # Check the cards collection requires an additional step, so check data_key first
    #     new_items = self.get_items_to_add()
    #     if new_items.__len__() > 0:
    #         print(f"New {self.get_collection_name()} data available")
    #             # if collection.count_documents(
    #             #     {collection_key[data_key]: {
    #             #          "scryfallOracleId": item
    #             #      }}) == 0:
    #             #     print(
    #             #         "%s is not currently in DB (looked for { %s: { %s }})"
    #             #         % (item, collection_key[data_key],
    #             #            collection_key[collection_key[data_key]]))
    #             #     new_item = {collection_key[collection_key[data_key]]: item}
    #             #     new_items.append(new_item)
    #     # else:
    #     #     for item in latest_data[data_key]:
    #     #         if collection.count_documents({collection_key[data_key]:
    #     #                                        item}) == 0:
    #     #             print(
    #     #                 "%s is not currently in DB (looked for { %s })" %
    #     #                 (item, collection_key[data_key]))
    #     #             new_item = {collection_key[data_key]: item}
    #     #             new_items.append(new_item)
    #     end = datetime.now()
    #     print("Total Time to check DB:", end - start)
    #     return new_items

    def insert_new_items(self) -> dict:
        print("Inserting %s new items into DB..." %
                             (len(self.new_items)))
        start = datetime.now()
        db_results = {}

        ### Comment out the next 3 lines when testing to avoid writing to DB
        try:
            db_results = self.collection.insert_many(self.new_items)
            print("Added new items to DB:", db_results)
        except Exception as e:
            db_results = e
            print("Exception while inserting item to DB:", e)

        ### Use below print() statement for testing to avoid writing to DB
        # print("(TEST) Adding new items to DB: %s" % (str(self.new_items)))
        end = datetime.now()
        print("Total Time to insert items into DB:", end - start)
        return db_results

    def update_items(self) -> dict:
        print(f"Updating {self._capitalized_name} items in DB...")
        start = datetime.now()
        db_results = {}
        query = self.__build_update_query()
        ### Comment out the next 3 lines when testing to avoid writing to DB
        # try:
        #     db_results = self.collection.update_many(self.new_items)
        #     print("Updating items in DB:", db_results)
        # except Exception as e:
        #     db_results = e
        #     print("Exception while updating item in DB:", e)

        ### Use below print() statement for testing to avoid writing to DB
        print("(TEST) Adding new items to DB:", str(self.new_attributes))
        end = datetime.now()
        print("Total Time to update items in DB:", end - start)
        return db_results
