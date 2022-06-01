from curses import raw
import json
import os

from app.mtg_collections.update import IUpdater

class SetsUpdater(IUpdater):
    _capitalized_name = "Sets"
    _data_endpoint = "https://mtgjson.com/api/v5/SetList.json"
    _identifier = "code"        
    new_items = []
    new_attributes = []

    ###
    # Utility Methods
    ###
    def get_distinct_coll_items(self) -> list:
        return self.get_whole_collection().distinct(self._identifier)

    def get_items_to_add(self) -> list:
        self.new_items = []
        local_data = self.local.get_data()
        coll_items = self.get_distinct_coll_items()
        for set in local_data:
            print(set[self._identifier])
            if set not in coll_items:
                self.new_items.append({ "set": set })
        return self.new_items

    def get_items_to_update(self) -> list:
        self.new_attributes = []
        print("Only set codes are checked, so no additional data to update.")
        return self.new_attributes

    def update_local_data(self):
        print("Updating Sets data...")
        self.local.update()

    def handle_sets_update(self) -> None:
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
            print("Unable to remove temp data file", e, exc_info=True)