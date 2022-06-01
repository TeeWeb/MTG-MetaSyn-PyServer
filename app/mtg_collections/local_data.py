from abc import ABC
from itertools import chain
from zipfile import ZipFile
import requests
import json
import chunk

###
# Abstract class for local data interfaces
###
class ILocalData(ABC):
    _data_dir_path = './app/data/'

    def __init__(self, data_category, file_name, data_endpoint):
        self.category = data_category
        # Local data file "Getters"
        self.file_name = file_name
        self.get_date = self.__get_local_data_date
        self.get_data = self.__get_local_data
        # API Data for updates
        self.endpoint = data_endpoint
        
    ###
    # Private methods
    ###
    def __str__(self):
        return "\nLocal Data: {}\n  * Path: {}\n  * Date: {}".format(self.category, self._data_dir_path, self.get_date())

    # Methods for main local file used for storing and reformatting data before updating Mongo DB
    def __get_file_path(self) -> str:
        return self._data_dir_path + self.file_name

    def __get_local_data_date(self) -> str:
        print(self.__get_file_path())
        date = self.__get_date_from_file(self.__get_file_path())
        return date

    def __get_local_data(self) -> dict:
        try:
            data = self.__get_all_data_from_file(self.__get_file_path())
            return data[self.category]
        except FileNotFoundError:
            raise FileNotFoundError(self.__get_file_path() + " not found")

    # Methods for temporary/cached data from MTG API and checking for possible
    def __get_temp_data_file_name(self) -> str:
        return "new" + self.file_name

    def __get_temp_data_file_path(self) -> str:
        return self._data_dir_path + self.__get_temp_data_file_name()

    def __get_temp_data_date(self) -> str:
        try:
            date = self.__get_date_from_file(self.__get_temp_data_file_path())
            return date
        except FileNotFoundError:
            raise FileNotFoundError(self.__get_temp_data_file_path() + " not found")

    def __get_temp_data(self) -> dict:
        try:
            with open(self.__get_temp_data_file_path(), 'r') as f:
                data = json.loads(f.read())
            return data['data']
        except FileNotFoundError:
            raise FileNotFoundError(self.__get_temp_data_file_path() + " not found")
        
    # Generic methods for getting date or data from a given file
    def __get_date_from_file(self, path: str) -> str:
        date = None
        try:
            with open(path, 'r') as f: 
                data = json.loads(f.read())
                date = data['meta']['date']
        except FileNotFoundError as e:
            print("Exception: " + path + " not found. New data needed.", e)
            return e
        except KeyError as e:
            print("Exception: " + path + " is missing date metadata. New data needed.", e)
            return e
        except TypeError as e:
            print("Exception: Unable to retrieve date from local data (%s).%s" % (e, path))
            return e
        return date

    def __get_all_data_from_file(self, path: str) -> dict:
        try:
            with open(path, 'r') as f:
                data_dict = json.loads(f.read())
                return data_dict
        except Exception as e:
            raise Exception("Error attempting to open data file:", e)

    # Methods for formatting API data (as a dict that contains a list of dict items as one of the values) into needed structure based on category
    # Local Data should be returned in the following format: 
    #   {
    #       "meta": {
    #           "date": {date}
    #       }, {plural_category_name}: {formatted_data_}
    #   }
    def __format_keywords_data(self, data: dict) -> dict:
        print("Formatting cached Keywords data")
        flattened_iter = chain(data['abilityWords'], data['keywordAbilities'],
                               data['keywordActions'])
        keywords = []
        for i in flattened_iter:
            keywords.append(i)
        keywords.sort()
        # Combine metadata from cached data with flattened dict to create reformatted data dict
        reformatted_dict = { "meta" : { "date": self.__get_temp_data_date() }, "keywords": keywords}
        return reformatted_dict

    def __format_sets_data(self, data: dict) -> dict:
        print("Formatting cached Sets data")
        sets = []
        reformatted_dict = {"sets": []}
        for card_set in data:
            sets.append(card_set)
        sets.sort(key=lambda set: set['code'])
        reformatted_dict = { "meta" : { "date": self.__get_temp_data_date() }, "sets": sets}
        return reformatted_dict

    def __format_cards_data(self, data: dict) -> dict:
        print("Formatting cached Cards data")
        # AtomicCards data contains objects where:
        # KEY is a card name (key=<card_name>)
        # VALUE is an array of objects representing versions of cards with that name
        # Reformat into a list of card objects and create AtomicCards.json file
        cards = []
        for card_name in data:
            for card_version in data[card_name]:
                # Remove foreignData and printings from cardData to reduce size
                try:
                    del card_version['foreignData']
                except Exception as e:
                    # No 'foreignData' available
                    continue
                try:
                    del card_version['printings']
                except Exception as f:
                    # No 'printings' data available
                    continue
                # Create a hash of card's scryfallOracleId + faceName (if available) to create a unique "_id" value
                try:
                    card_version['_id'] = hash((card_version['identifiers']['scryfallOracleId'],
                            card_version['faceName']))
                except:
                    try:
                        card_version['scryfallOracleId'] = card_version['identifiers']['scryfallOracleId']
                        del card_version['identifiers']
                        card_version['_id'] = hash(card_version['scryfallOracleId'])
                        # cards[str(card_version['identifiers']['scryfallOracleId'])] = card_version
                        cards.append(card_version)
                    except:
                        print(f"{card_name} has no scryfallOracleId. Skipping card...")
                        continue
        reformatted_dict = { "meta" : { "date": self.__get_temp_data_date() }, "cards": cards}
        return reformatted_dict

    def __format_types_data(self, data: dict) -> dict:
        print("Formatting cached Types data")
        types = []
        for type in data:
            types.append({"type": type, "subTypes": data[type]['subTypes'], "superTypes": data[type]['superTypes']})
        reformatted_dict = { "meta" : { "date": self.__get_temp_data_date() }, "types": types}
        return reformatted_dict

    def __format_data(self, data: dict) -> dict:
        switch = {
            "keywords": self.__format_keywords_data,
            "types": self.__format_types_data,
            "cards": self.__format_cards_data,
            "sets": self.__format_sets_data
        }
        try:
            formatted_data = switch.get(self.category)
            return formatted_data(data)
        except Exception as e:
            Exception("Unable to format data:", e)

    def __get_item_from_local_data(self, key, item_id: str) -> dict:
        local_data = self.get_data()
        item_dict = local_data[key][item_id]
        return item_dict 

    # GET data from API, reformat (if needed), and store in temp data file (w/ "new" prefix) 
    def __fetch_latest_api_data(self) -> requests.Request:
        print('Requesting =>', self.endpoint)
        try:
            if self.__get_temp_data_file_path().split(".").pop() == "zip":
                r = requests.get(self.endpoint, headers={"Content-Type": "application/zip"})
            else: 
                r = requests.get(self.endpoint, stream=True)
            if r.status_code == 200:
                return r
            else:
                raise Exception("Failed to retrieve new API data. Response code is not '200':", r.status_code)
        except Exception as e:
            print(e)

    def __save_data_locally(self, req_data: chunk, chunk_size=128) -> None:
        print("Saving data to", str(self.__get_temp_data_file_path()))
        try:
            # Handle .zip files, if applicable
            if self.__get_temp_data_file_path().split(".").pop() == "zip":
                print("Extracting zipped file(s)")
                ZipFile.extractall(self.__get_temp_data_file_path())
            with open(self.__get_temp_data_file_path(), 'wb') as f:
                for chunk in req_data.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        except Exception as e:
            print(e)

    def __is_outdated(self) -> bool:
        print("\nChecking for new '" + self.category + "' data...")
        last_updated = self.get_date()
        if type(last_updated) == FileNotFoundError:
            print("No local data saved. Fetching latest data for cache.")
            latest = self.__fetch_latest_api_data()
            self.__save_data_locally(latest)
            return True
        cached = self.__get_temp_data_date()
        if type(cached) == FileNotFoundError:
            print("No date from Cached Data. Pulling new latest API data.")
            latest = self.__fetch_latest_api_data()
            self.__save_data_locally(latest)
            cached = self.__get_temp_data_date()
        if str(last_updated) < str(cached):
            print("Cached data is newer than last update. Last Updated: %s Cached: %s" % (last_updated, cached))
            return True
        print("No updates available. Last Updated: %s Cached: %s" % (last_updated, cached))
        return False

    def update(self) -> None:
        if self.__is_outdated():
            print("Updating local data...")
            # Format raw API data into needed structure
            formatted_data = self.__format_data(self.__get_temp_data())
            try: # Save formatted data into local file for further use
                with open(self.__get_file_path(), 'w') as f:
                    f.write(json.dumps(formatted_data, indent=4))
            except Exception as e:
                Exception("Error while saving cached data to local file:", e)
            # 4) Pass new data to DB Manager
            # 5) Replace local data with temp data 
            pass
        