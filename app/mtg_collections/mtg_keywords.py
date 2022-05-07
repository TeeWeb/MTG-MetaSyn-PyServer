from itertools import chain
import requests
import json
import os

from app.mtg_collections.update import IUpdater

class KeywordsUpdater(IUpdater):
    _capitalized_name = "Keywords"
    _collection_name = "keywords"
    _data_endpoint = "https://mtgjson.com/api/v5/Keywords.json"

    ###
    # Private Methods
    ###
    def __flatten_keywords_lists(data: dict) -> dict:
        flattened_iter = chain(data['abilityWords'], data['keywordAbilities'],
                               data['keywordActions'])
        keywords = []
        for i in flattened_iter:
            keywords.append(i)
        keywords.sort()
        sorted_keywords = dict.fromkeys(keywords)
        return sorted_keywords
    
    ###
    # Utility Methods
    ###
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
            with open(self.data_dir_path + 'Keywords.json', 'w') as f:
                f.write(json.dumps(keyword_data, indent=4))
            # Compare most recent list of keywords with DB Collection
            collection = self.get_db_collection('keywords')
            # Insert any new keywords that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new keywords BEFORE they're inserted into database
            new_items = self.get_new_items(self, collection, keyword_data,
                                          'keywords')
            self.insert_new_items(self, collection, new_items)
        # Remove temp newKeywords.json file now that Keywords.json file has been updated
        try:
            os.remove(self.data_dir_path + 'newKeywords.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def cache_data_from_api(self) -> str:
        raw_data = self.get_data_from_api.json()
        formatted_data = self.__flatten_keywords_lists(raw_data)
        with open(self.local_data.get_temp_file_path(), 'w') as f:
            f.write(json.dumps(formatted_data, indent=4))
            f.close()
        return formatted_data["meta"]["data"]

    def update_local_data(self):
        print("Updating local Keywords data...")
        # new_data = self.__request_data()
        # print("Request:", new_data.status_code)
        # self.__flatten_keywords_lists(new_data)