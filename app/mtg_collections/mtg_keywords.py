from itertools import chain
import requests
import json
import os

from app.mtg_collections.update import IUpdater

class KeywordsUpdater(IUpdater):
    _capitalized_name = "Keywords"
    _collection_name = "keywords"
    _data_endpoint = "https://mtgjson.com/api/v5/Keywords.json"
    _identifier = "keyword"       
    new_items = []
    new_attributes = [] 

    def _is_new_keyword(self, keyword) -> bool:
        if self.collection.find_one({ self._identifier: keyword }):
            return False
        return True
    
    ###
    # Utility Methods
    ###
    def get_items_to_add(self) -> list:
        self.new_items = []
        local_data = self.local.get_data()
        for keyword in local_data:
            if self._is_new_keyword(keyword):
                self.new_items.append({ self._identifier: keyword })
        return self.new_items

    def get_items_to_update(self) -> list:
        self.new_attributes = []
        print("No additional keywords data to update")
        return self.new_attributes

    def handle_keywords_update(self):    
        # Check release date of latest data for any updates
        print("--- Keywords ---")
        if self.local_update_needed(self, self.file_name,
                                   self._data_endpoint):
            # Get latest keywords from newKeywords.json
            # TODO: Use 'newKeywords.json' that has already been featched rather than requesting again
            new_data = requests.get(
                'https://mtgjson.com/api/v5/Keywords.json').json()
            sorted_keywords = self.flatten_keywords_lists(new_data['data'])
            last_data_update = new_data['meta']['date']
            keyword_data = {
                "meta": {
                    "date": last_data_update
                },
                "keywords": sorted_keywords
            }
            with open(self._data_dir_path + 'Keywords.json', 'w') as f:
                f.write(json.dumps(keyword_data, indent=4))
            # Compare most recent list of keywords with DB Collection
            collection = self.get_db_collection('keywords')
            # Insert any new keywords that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new keywords BEFORE they're inserted into database
            new_items = self.get_items_to_add(self, collection, keyword_data,
                                          'keywords')
            self.insert_new_items(self, collection, new_items)
        # Remove temp newKeywords.json file now that Keywords.json file has been updated
        try:
            os.remove(self._data_dir_path + 'newKeywords.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def update_local_data(self):
        print("Updating local Keywords data...")
        self.local.update()
        # new_data = self.__request_data()
        # print("Request:", new_data.status_code)
        # self.__flatten_keywords_lists(new_data)