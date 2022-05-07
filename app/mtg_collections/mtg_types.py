import json
import requests
import os

from app.mtg_collections.update import IUpdater

class TypesUpdater(IUpdater):
    _capitalized_name = "Types"
    _collection_name = "types"
    _data_endpoint = "https://mtgjson.com/api/v5/Keywords.json"

    def __get_new_data_pulled_from_api(self) -> dict:
        try:
            with open(self.local_data.get_temp_file_path(), 'r') as f:
                data_dict = json.loads(f.read())
                return data_dict
        except Exception as e:
            Exception("Error attempting to open temporary data file:", e)

    def print_date_last_updated(self) -> str:
        return self.print_date_from_local_data()

    def local_update_needed(self) -> bool:
        return self.__is_local_update_needed(self.last_updated())

    def handle_types_update(self):
        print("--- Types ---")
        # Check release date of latest data for any updates
        if self.local_update_needed(
                self, self.file_name,
                self._data_endpoint):
            # Get latest card types
            # TODO: Use 'newCardTypes.json' that has already been featched rather than requesting again
            raw_data = requests.get(self._data_endpoint).json()
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
            with open(self.data_dir_path + '/CardTypes.json', 'w') as f:
                f.write(json.dumps(types_data, indent=4))
            collection = self.get_db_collection('types')
            # Compare most recent list of types with DB Collection
            # Insert any new types that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new types BEFORE they're inserted into database
            new_items = self.get_new_items(self, collection, types_data, 'types')
            for new_item in new_items:
                new_item["subtypes"] = types_data['types'][new_item['type']]
            self.insert_new_items(self, collection, new_items)
        # Remove temp newCardTypes.json file now that CardTypes.json file has been updated
        try:
            os.remove(self.data_dir_path + 'newCardTypes.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def cache_data_from_api(self) -> str:
        raw_data = self.get_data_from_api.json()
        with open(self.local_data.get_temp_file_path(), 'w') as f:
            f.write(json.dumps(raw_data, indent=4))
            f.close()
        return raw_data["meta"]["data"]

    def update_local_data(self):
        print("Updating Types data...")
        pass