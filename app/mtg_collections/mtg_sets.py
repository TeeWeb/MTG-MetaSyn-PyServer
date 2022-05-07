from curses import raw
import json
import os
from typing import Collection, Dict
from urllib.request import Request

from app.mtg_collections.update import IUpdater

class SetsUpdater(IUpdater):
    _capitalized_name = "Sets"
    _collection_name = "sets"
    _data_endpoint = "https://mtgjson.com/api/v5/SetList.json"

    ###
    # Private Methods
    ###
    def __format_cached_sets_data(self, cached_data) -> dict:
        sets = []
        set_codes = {"sets": []}
        for card_set in cached_data['data']:
            sets.append(card_set)
            set_codes['sets'].append(card_set['code'])
        sets.sort(key=lambda set: set['code'])
        last_data_update = cached_data['meta']['date']
        return {"meta": {"date": last_data_update}, "sets": sets}

    ###
    # Utility Methods
    ###
    def handle_sets_update(self):
        print("--- Sets ---")
        collection = self.get_collection()
        # Check release date of latest data for any updates
        if self.__is_local_update_needed(self.file_name, self._data_endpoint):
            sets_data = self.latest_data()
            self.__update_local_data(sets_data)
            # TODO: add function to run synergy calculator on new sets BEFORE they're inserted into database
            filtered_new_data = self.__filter_for_new_data(collection, sets_data)
            print(filtered_new_data)
            # Insert all new set objects into DB
            # self.insert_new_items(self, collection, filtered_new_data)
        # Remove temp newSets.json file now that sets.json file has been updated
        try:
            os.remove(self.manager.data_dir_path + 'newSetList.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def cache_data_from_api(self) -> str:
        raw_data = self.get_data_from_api().json()
        formatted_data = self.__format_cached_sets_data(raw_data)
        with open(self.local_data.get_temp_file_path(), 'w') as f:
            f.write(json.dumps(formatted_data, indent=4))
            f.close()
        return formatted_data["meta"]["date"]

    def update_local_data(self):
        print("Updating Sets data...")
        # self.cache_data_from_api()
        # try:
        #     with open(self.local_data.get_temp_file_path(), 'r') as e:
        #         cached_data = json.loads(e.read())
        # except Exception as err1:
        #     raise Exception("Error while loading cached '" + self._collection_name + "' file at " + self.local_data.get_temp_file_path + ":", err1)
        # try:
        #     with open(self.local_data.get_file_path(), 'w') as f:
        #         f.write(json.dumps(cached_data, indent=4))
        #         f.close()
        # except Exception as err2:
        #     raise Exception("Error while writing cached '" + self._collection_name + "' sets data to local data file at " + self.local_data.get_file_path + ":", err2)
