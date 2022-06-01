import json
import requests
import os

from app.mtg_collections.update import IUpdater

def has_subtype(type_obj) -> bool:
    return type_obj['subTypes'].__len__() > 0

def has_supertype(type_obj) -> bool:
    return type_obj['superTypes'].__len__() > 0

class TypesUpdater(IUpdater):
    _capitalized_name = "Types"
    _data_endpoint = "https://mtgjson.com/api/v5/CardTypes.json"
    _identifier = "type"
    new_items = []
    new_attributes = []

    def __get_type_from_local_data(self, type) -> dict:
        local_data = self.local.get_data()
        return local_data['types'][type]

    def __get_subtypes_from_local_data(self, type) -> list:
        local_data = self.local.get_data()
        subtypes = local_data[type]['subTypes']
        return subtypes

    def __get_supertypes_from_local_data(self, type) -> list:
        local_data = self.local.get_data()
        supertypes = local_data[type]['superTypes']
        return supertypes

    def __get_type_from_cloud_date(self, type) -> dict:
        count = self.collection.count_documents({ "type": type })
        if count == 1:
            type_from_db = self.collection.find_one({ "type": type })
            return type_from_db
        elif count < 1:
            raise Exception("Type not found in DB:", type)
        else:
            raise Exception("More than one %s type found." % type)

    def __get_subtypes_from_cloud_data(self, type) -> list:
        count = self.collection.count_documents({ self._identifier: type })
        if count > 1:
            raise Exception("More than one %s type found in MongoDB %s collection" % (type, self._collection_name))
        elif count < 1:
            return []
        else:
            try:
                cloud_data = self.collection.find_one({ self._identifier: type })
                subtypes = cloud_data['subtypes']
                return subtypes
            except KeyError:
                return []
            except Exception as e:
                Exception("Error while accessing %s subtypes in cloud data" % (type))

    def __get_supertypes_from_cloud_data(self, type) -> list:
        count = self.collection.count_documents({ self._identifier: type })
        if count > 1:
            raise Exception("More than one %s type found in MongoDB %s collection" % (type, self._collection_name))
        elif count < 1:
            return []
        else:
            try:
                cloud_data = self.collection.find_one({ self._identifier: type })
                supertypes = cloud_data['supertypes']
                return supertypes
            except KeyError:
                return []
            except Exception as e:
                Exception("Error while accessing %s supertypes in cloud data" % (type))

    def __get_new_subtypes(self, type) -> list:
        local_subtypes = self.__get_subtypes_from_local_data(type)
        mongodb_subtypes = self.__get_subtypes_from_cloud_data(type)
        new_subtypes = []
        for s in local_subtypes:
            if s not in mongodb_subtypes:
                new_subtypes.append(s)
        return new_subtypes

    def __get_new_supertypes(self, type) -> list:
        local_supertypes = self.__get_supertypes_from_local_data(type)
        mongodb_supertypes = self.__get_supertypes_from_cloud_data(type)
        new_supertypes = []
        for s in local_supertypes:
            if s not in mongodb_supertypes:
                new_supertypes.append(s)
        return new_supertypes

    def get_distinct_coll_items(self) -> list:
        return self.get_whole_collection().distinct(self._identifier)

    def get_items_to_add(self) -> list:
        self.new_items = []
        local_data = self.local.get_data()
        coll_items = self.get_distinct_coll_items()
        for cached_item in local_data:
            print(cached_item)
            if cached_item not in coll_items:
                self.new_items.append({ self._identifier: cached_item })
        return self.new_items

    ###
    # Utility Methods
    ###
    def print_date_last_updated(self) -> str:
        return self.print_date_from_local_data()

    def local_update_needed(self) -> bool:
        return self.__is_local_update_needed(self.last_updated())

    def get_items_to_update(self) -> list:
        self.new_attributes = []
        local_data = self.local.get_data()
        for type in local_data:
            # If 'type' is already present in DB
            if self.collection.find_one({ "type": type }):
                if has_subtype(local_data[type]):
                    # Check for new subtypes
                    new_subtypes = self.__get_new_subtypes(type)
                    if new_subtypes.__len__() > 0:
                        self.new_attributes.append({"type": type, "subtypes": new_subtypes})
                if has_supertype(local_data[type]):
                    # Check for new supertypes
                    new_supertypes = self.__get_new_supertypes(type)     
                    if new_supertypes.__len__() > 0:
                        self.new_attributes.append({"type": type, "supertypes": new_supertypes})
        return self.new_attributes

    def handle_types_sync(self):
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
            with open(self._data_dir_path + '/CardTypes.json', 'w') as f:
                f.write(json.dumps(types_data, indent=4))
            collection = self.get_db_collection('types')
            # Compare most recent list of types with DB Collection
            # Insert any new types that are not already in the DB Collection
            # TODO: add function to run synergy calculator on new types BEFORE they're inserted into database
            new_items = self.get_items_to_add(self, collection, types_data, 'types')
            for new_item in new_items:
                new_item["subtypes"] = types_data['types'][new_item['type']]
            self.insert_new_items(self, collection, new_items)
        # Remove temp newCardTypes.json file now that CardTypes.json file has been updated
        try:
            os.remove(self._data_dir_path + 'newCardTypes.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def update_local_data(self):
        print("Updating Types data...")
        self.local.update()