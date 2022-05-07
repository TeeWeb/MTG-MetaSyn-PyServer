from abc import ABCMeta
import json
import chunk

###
# Abstract class for local data interfaces
###
class ILocalData(metaclass=ABCMeta):
    def __init__(self, data_dir_path, data_category, file_name):
        self.data_dir_path = data_dir_path
        self.category = data_category
        # Local data file "Getters"
        self.get_file_name = file_name
        self.get_file_path = self.__get_file_path
        self.get_date = self.__get_local_data_date
        self.get_data = self.__get_local_data
        # Temp data file "Getters"
        self.get_temp_file_name = self.__get_temp_data_file_name
        self.get_temp_file_path = self.__get_temp_data_file_path
        self.get_temp_data_date = self.__get_temp_data_date
        
        self.update_local_data = self.update

    ###
    # Private methods
    ###
    def __str__(self):
        return "\nLocal Data: {}\n  * Path: {}\n  * Date: {}".format(self.category, self.data_dir_path, self.get_date())

    # Main file for storing and reformatting data before updating Mongo DB
    def __get_file_path(self) -> str:
        return self.data_dir_path + self.get_file_name

    def __get_local_data_date(self) -> str:
        date = self.__get_date_from_file(self.get_file_path())
        return date

    def __get_local_data(self) -> dict:
        try:
            data =self.__get_all_data_from_file(self.get_file_path())
            return data
        except FileNotFoundError:
            raise FileNotFoundError(self.get_file_path + " not found")

    # Temporary file for caching latest data from MTG API and checking for possible updates
    def __get_temp_data_file_name(self) -> str:
        return "new" + self.get_file_name

    def __get_temp_data_file_path(self) -> str:
        return self.data_dir_path + self.get_temp_file_name()

    def __get_temp_data_date(self) -> str:
        try:
            date = self.__get_date_from_file(self.get_temp_file_path())
            return date
        except FileNotFoundError:
            raise FileNotFoundError(self.get_temp_file_path + " not found")

    def __get_temp_data(self) -> dict:
        try:
            data = self.__get_date_from_file(self.get_temp_file_path())
            return data
        except FileNotFoundError:
            raise FileNotFoundError(self.get_temp_file_path + " not found")
        
    def __get_date_from_file(self, path) -> str:
        date = None
        try:
            with open(path, 'r') as f: 
                data = json.loads(f.read())
                date = data['meta']['date']
        except FileNotFoundError as e:
            print("Exception: " + path + " not found. New data needed.", e)
        except KeyError as e:
            print("Exception: " + path + " is missing date metadata. New data needed.", e)
        except TypeError as e:
            print("Exception: Unable to retrieve date from local data (%s).%s" % (e, path))
        return date

    def __get_all_data_from_file(self, path) -> dict:
        try:
            with open(path, 'r') as f:
                data_dict = json.loads(f.read())
                return data_dict
        except Exception as e:
            raise Exception("Error attempting to open data file:", e)

    ###
    # Utility Methods
    ###
    def save_data_locally(self, req_data: chunk, save_path, chunk_size=128) -> str:
        print("Saving data to", str(save_path))
        try:
            with open(save_path, 'wb') as f:
                for chunk in req_data.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
            with open(save_path, 'r') as data:
                latest_data = json.loads(data.read())
                return latest_data['meta']['date']
        except Exception as e:
            print(e)

    def update(self):
        print("Updating local data...")
        pass