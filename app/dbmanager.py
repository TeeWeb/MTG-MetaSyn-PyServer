import argparse
import chunk
from email.parser import Parser
import json
from multiprocessing import Manager
from socket import SHUT_WR
from typing import Dict, Tuple
from warnings import WarningMessage
from pip import List
import requests
from yaml import load, Loader

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
    initialized_updaters = {}

    def __init__(self, config: dict=get_default_config()):
        self.config = config
        self.get_client = self.__get_mongodb_client
        self.keywords = KeywordsUpdater()
        self.types =  TypesUpdater()
        self.sets = SetsUpdater()
        self.cards = CardsUpdater()
        self.updaters = {
            "keywords": self.keywords,
            "types": self.types,
            "sets": self.sets,
            "cards": self.cards
        }
        self.outdated_collections = self.__check_for_updates
    
    ###
    # Private methods
    ###
    def __str__(self):
        return print("-- Update Manager --\nDB URL: %s\nDB Username: %s\n" %
             (self.__get_db_uri(), self.__get_username()))

    def __get_username(self) -> str:
        return self.config['username']
    
    def __get_db_uri(self) -> str: 
        return self.config['db_uri']

    def __get_user_pw(self) -> str:
        return self.config['pw']

    def __get_mongodb_client(self) -> MongoClient:
        print("%s connecting to DB at %s via %s" % (self.__get_username(), self.__get_db_uri(), id(self)))
        try:
            client = MongoClient(
                "mongodb+srv://%s:%s@%s?retryWrites=true&w=majority" %
                (self.__get_username(), self.__get_user_pw(), self.__get_db_uri()))
        except ConnectionError as e:
            print("Unable to connect to DB", e)
            return
        db = client['MetaSynDB']
        return db

    def __get_date(self, updater: IUpdater) -> str:
        date = updater.get_date()
        return date

    def __get_all_dates(self) -> Dict:
        dates = {}
        for i in self.updaters.keys():
            name = i
            last_updated = self.__get_date(self.updaters[i])
            dates[name] = last_updated
        return dates

    def __check_for_updates(self) -> List:
        outdated_collections = {}
        for coll in self.updaters:
            if self.updaters[coll].is_outdated():
                outdated_collections[coll] = self.updaters[coll].get_date()
        return outdated_collections

    ###
    # Utility Methods
    ###
    def print_all_dates(self):
        dates = self.__get_all_dates()
        print("\n-- Dates Last Updated --\nCards: %s\nSets: %s\nTypes: %s\nKeywords: %s\n" % (
            dates["cards"], 
            dates["sets"], 
            dates["types"], 
            dates["keywords"])
        )

    def print_help(self):
        print("\n-- MetaSynDB Manager: Manual --\nAvailable commands:\n\n" + 
            "exit - Close the DB Manager cli.\n" +
            "get dates - Prints dates that each db collection was last updated.\n" +
            "update all - Updates all db collections.\n\n"),

    def update(self, data_set: str):
        print("Updating %s data" % data_set)
        try:
            updater = self.updaters[data_set]
            updater.update_local_data()
        except Exception as e:
            if type(e) is KeyError:
                print("Invalid data set entered. Please enter a valid option:", self.updaters.keys())

    def update_all(self):
        print("Updating All Collections...")
        for i in self.updaters.keys():
            updater = self.updaters[i]
            updater.update_local_data()


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


