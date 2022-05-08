from abc import ABC
from itertools import chain
import json
import chunk

###
# Abstract class for local data interfaces
###
class ILocalData(ABC):
    _data_dir_path = './app/data/'

    def __init__(self, data_category, file_name):
        self.category = data_category
        # Local data file "Getters"
        self.file_name = file_name
        self.get_file_path = self.__get_file_path
        self.get_date = self.__get_local_data_date
        self.get_data = self.__get_local_data
        # Temp data file "Getters"
        self.get_temp_file_name = self.__get_temp_data_file_name
        self.get_temp_file_path = self.__get_temp_data_file_path
        self.get_temp_data_date = self.__get_temp_data_date
        self.get_temp_data = self.__get_temp_data
        

    ###
    # Private methods
    ###
    def __str__(self):
        return "\nLocal Data: {}\n  * Path: {}\n  * Date: {}".format(self.category, self._data_dir_path, self.get_date())

    # Methods for main local file used for storing and reformatting data before updating Mongo DB
    def __get_file_path(self) -> str:
        return self._data_dir_path + self.file_name

    def __get_local_data_date(self) -> str:
        date = self.__get_date_from_file(self.get_file_path())
        return date

    def __get_local_data(self) -> dict:
        try:
            data = self.__get_all_data_from_file(self.get_file_path())
            return data[self.category]
        except FileNotFoundError:
            raise FileNotFoundError(self.get_file_path() + " not found")

    # Methods for temporary/cached data from MTG API and checking for possible
    def __get_temp_data_file_name(self) -> str:
        return "new" + self.file_name

    def __get_temp_data_file_path(self) -> str:
        return self._data_dir_path + self.get_temp_file_name()

    def __get_temp_data_date(self) -> str:
        try:
            date = self.__get_date_from_file(self.get_temp_file_path())
            return date
        except FileNotFoundError:
            raise FileNotFoundError(self.get_temp_file_path() + " not found")

    def __get_temp_data(self) -> dict:
        try:
            with open(self.__get_temp_data_file_path(), 'r') as f:
                data = json.loads(f.read())
            return data['data']
        except FileNotFoundError:
            raise FileNotFoundError(self.get_temp_file_path() + " not found")
        
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

    def __format_keywords_data(self, data: dict) -> dict:
        print("Formatting cached Keywords data")
        flattened_iter = chain(data['abilityWords'], data['keywordAbilities'],
                               data['keywordActions'])
        keywords = []
        for i in flattened_iter:
            keywords.append(i)
        keywords.sort()
        # Combine metadata from cached data with flattened dict to create reformatted data dict
        reformatted_dict = { "meta" : { "date": self.get_temp_data_date() }, "keywords": keywords}
        return reformatted_dict

    def __format_sets_data(self, data: dict) -> dict:
        print("Formatting cached Sets data")
        sets = []
        reformatted_dict = {"sets": []}
        for card_set in data:
            sets.append(card_set)
        sets.sort(key=lambda set: set['code'])
        reformatted_dict = { "meta" : { "date": self.get_temp_data_date() }, "sets": sets}
        return reformatted_dict

    def __format_cards_data(self, data: dict) -> dict:
        print("Formatting cached Cards data")
        # AtomicCards data contains objects where:
        # KEY is a card name (key=<card_name>)
        # VALUE is an array of objects representing versions of cards with that name
        # Build a list of card versions and create AtomicCards.json file
        cards = []
        oracle_ids = {"cards": []}
        for card_name in data:
            print(card_name)
            for card_version in data[card_name]:
                # Remove foreignData and printings from cardData to reduce size
                try:
                    del card_version['foreignData']
                except Exception as e:
                    print("Exception:", e)
                try:
                    del card_version['printings']
                except Exception as f:
                    print("Exception:", f)
                # Create a hash of card's faceName + scryfallOracleId to create a unique "_id" value
                try:
                    card_version['_id'] = hash((card_version['identifiers']['scryfallOracleId'],
                            card_version['faceName']))
                except:
                    try:
                        card_version['_id'] = hash(card_version['identifiers']['scryfallOracleId'])
                        cards.append(card_version)
                        oracle_ids['cards'].append(str(card_version['identifiers']['scryfallOracleId']))
                    except:
                        print("%s has no scryfallOracleId" % card_name)
                        continue
        reformatted_dict = { "meta" : { "date": self.get_temp_data_date() }, "cards": cards}
        return reformatted_dict

    def __format_types_data(self, data: dict) -> dict:
        print("Formatting cached Types data")
        reformatted_dict = { "meta" : { "date": self.get_temp_data_date() }, "types": data}
        return reformatted_dict

    def __get_formatted_data(self, data: dict) -> dict:
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

    ###
    # Utility Methods
    ###
    def save_data_locally(self, req_data: chunk, chunk_size=128) -> None:
        print("Saving data to", str(self.get_temp_file_path()))
        try:
            with open(self.get_temp_file_path(), 'wb') as f:
                for chunk in req_data.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        except Exception as e:
            print(e)

    def update(self):
        print("Updating local data...")
        reformatted_data = self.__get_formatted_data(self.__get_temp_data())
        try:
            with open(self.get_file_path(), 'w') as f:
                f.write(json.dumps(reformatted_data, indent=4))
        except Exception as e:
            Exception("Error while saving cached data to local file:", e)
        # 4) Pass new data to DB Manager
        # 5) Replace local data with temp data 
        pass