import argparse
import json
from multiprocessing import Manager
from types import NoneType
from typing import Collection, Dict
from warnings import WarningMessage

# Updater Singletons
from app.mtg_collections.mtg_keywords import KeywordsUpdater
from app.mtg_collections.mtg_cards import CardsUpdater
from app.mtg_collections.mtg_sets import SetsUpdater
from app.mtg_collections.mtg_types import TypesUpdater

from pymongo import MongoClient

from app.mtg_collections.update import IUpdater

def get_default_config() -> dict:
    with open('./config.yaml', 'r') as f:
        default_config = json.loads(f.read())
    return default_config

###
# Main class for using Updaters, pulling updated data, and managing DB data
# Must pass a valid username to instantiate and connect with Mongo DB
###
class Manager():

    def __init__(self, config: dict=get_default_config()):
        self.config = config
        self.client = self.__get_mongodb_client()
        self.keywords = self.__get_keywords_collection()
        self.types = self.__get_types_collection()
        self.sets = self.__get_sets_collection()
        self.cards = self.__get_cards_collection()
        self.updaters = {
            "keywords": self.keywords,
            "types": self.types,
            "sets": self.sets,
            "cards": self.cards
        }
    
    ###
    # Private methods
    ###
    def __str__(self):
        return print("-- Update Manager --\nDB URL: %s\nDB Username: %s\n" %
             (self.__get_db_uri(), self.__get_username()))

    def __get_username(self) -> str:
        return self.config['username']
    
    def __get_db_uri(self) -> str: 
        return self.config['mongodb_uri']

    def __get_user_pw(self) -> str:
        return self.config['pw']

    def __get_mongodb_client(self) -> MongoClient:
        print(f"{self.__get_username()} connecting to DB at {self.__get_db_uri()}")
        try:
            client = MongoClient(
                "mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                (self.__get_username(), self.__get_user_pw(), self.__get_db_uri()))
        except ConnectionError as e:
            print(f"Unable to connect to DB: {e}\nPlease try again.")
            return
        except Exception as f:
            raise Exception(f)
        print(f"{self.__get_username()} connected to {client}")
        db = client['MetaSynDB']
        return db

    def __get_all_dates(self) -> Dict:
        dates = {}
        for i in self.updaters:
            name = i
            if self.updaters[i] is None:
                raise Exception(f"{i} Updater() has not been initialized. Run connect() method and then try again.")
            last_updated = self.updaters[i].local.get_date()
            dates[name] = last_updated
        return dates

    def __get_mongodb_collection(self, collection_name: str) -> Collection:
        print(f"Initializing {collection_name}...")
        db_collection = self.client[collection_name]
        return db_collection

    def __get_keywords_collection(self) -> IUpdater:
        return KeywordsUpdater(self.__get_mongodb_collection('keywords'))

    def __get_types_collection(self) -> IUpdater:
        return TypesUpdater(self.__get_mongodb_collection('types'))

    def __get_sets_collection(self) -> IUpdater:
        return SetsUpdater(self.__get_mongodb_collection('sets'))

    def __get_cards_collection(self) -> IUpdater:
        return CardsUpdater(self.__get_mongodb_collection('cards'))

    ###
    # Utility Methods
    ###
    def switch_user(self, username, pw) -> None:
        print(f"Switching user to {username}")
        current_username = self.config['username']
        current_pw = self.config['pw']
        new_username = username
        new_pw = pw
        self.config['username'] = new_username
        self.config['pw'] = new_pw
        try:
            self.connect()
        except Exception as e:
            Exception("Unable to switch to new user.", e)
            self.config['username'] = current_username
            self.config['pw'] = current_pw

    def print_all_dates(self) -> NoneType:
        dates = self.__get_all_dates()
        print("\n-- Dates Last Updated --\nCards: %s\nSets: %s\nTypes: %s\nKeywords: %s\n" % (
            dates["cards"], 
            dates["sets"], 
            dates["types"], 
            dates["keywords"])
        )

    def print_help(self) -> NoneType:
        print("\n-- MetaSynDB Manager: Manual --\nAvailable commands:\n\n" + 
            "exit - Close the DB Manager cli.\n" +
            "get dates - Prints dates that each db collection was last updated.\n" +
            "update all - Updates all db collections.\n\n"),

    def __get_outdated_collections(self) -> dict:
        outdated_collections = {}
        for coll in self.updaters:
            if self.updaters[coll].is_outdated():
                outdated_collections[coll] = self.updaters[coll].local.get_date()
        if outdated_collections.__len__() == 0:
            return None
        else:
            return outdated_collections

    def update(self, data_set: str) -> NoneType:
        print("Updating %s data" % data_set)
        try:
            updater = self.updaters[data_set]
            updater.update_local_data()
        except Exception as e:
            if type(e) is KeyError:
                print("Invalid data set entered. Please enter a valid option:", self.updaters.keys())
            else:
                raise Exception("Error while updating %s data set: %s" % (data_set, e))

    def update_all(self) -> NoneType:
        print("\n--- Updating All Collections ---")
        outdated_collections = self.__get_outdated_collections()
        if outdated_collections:
            for i in outdated_collections:
                print("\n%s data is out of date. Updating %s Collection" % (i, i))
                updater = self.updaters[i]
                updater.update_local_data()
        else:
            print("No collections to update. All data is up-to-date.")
            self.print_all_dates()
        for upd8r in self.updaters:
            self.updaters[upd8r].sync()


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enter args for DB Manager")
    parser.add_argument("-m", "--mongodb_uri", dest="mongodb_uri", type=str, default=default_config['mongodb_uri'], help="Path to config file with MongoDB client info (i.e. url, username, password)")
    parser.add_argument("-u", "--username", dest="username", type=str, default=default_config['username'], help="Username for MongoClient access to MetaSynDB")
    parser.add_argument("-p", "--password", dest="password", type=str, default=default_config['pw'], help="Password for MongoClient access to MetaSynDB")
    parser.add_argument("-s", "--start", dest="start_cli", action="store_true", help="Starts interactive command line application for performing DB management tasks")
    parser.add_argument("-a", "--update_all", dest="auto_update_all", action="store_true", help="Updates all db collections")
    return parser

if __name__ == "__main__":
    default_config = get_default_config()
    parser = get_parser()
    args = parser.parse_args()

    if args.auto_update_all:
        print("Running UpdateAll()...")
    elif args.start_cli:
        prompt = "MTG MetaSynDB Manager => "
        user_input = input(prompt)
        if user_input != "exit":
            mgr = Manager({"db_uri": args.mongodb_uri, "username": args.username, "pw": args.password})
            while user_input != "exit":
                switch = {
                    "help": mgr.print_help,
                    "get dates": mgr.print_dates
                }
                try:
                    cmd = switch.get(user_input)
                except Exception as e:
                    raise WarningMessage("Invalid command.")
                cmd()
                user_input = input(prompt)
        exit(1)
    else:
        print("No option entered. Please provide [-s | --start_cli] or [-a | --update_all] option.")
    exit(1)


