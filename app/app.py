import requests
from yaml import load, Loader
from mtgsdk import Card

from app.synergyCalc import CalculatedSynergy


def create_app(test_config=None):

    def retrieve_all_types():
        print("Downloading MTG types data...")
        types_req = requests.get('https://api.magicthegathering.io/v1/types')
        if types_req.status_code == 200:
            with open('./data/types.yaml', 'w') as f:
                f.write(str(types_req.json()))
        else:
            print("types_req status code: ", types_req.status_code)

    def retrieve_all_sets():
        print("Downloading MTG sets data...")
        sets_req = requests.get('https://api.magicthegathering.io/v1/sets')
        if sets_req.status_code == 200:
            with open('./data/sets.yaml', 'w') as f:
                f.write(str(sets_req.json()))
        else:
            print("sets_req status code: ", sets_req.status_code)

    def retrieve_all_keywords():
        print("Downloading MTG keywords data...")
        keywords_req = requests.get('https://mtgjson.com/api/v5/Keywords.json')
        if keywords_req.status_code == 200:
            with open('./data/keywords.yaml', 'w') as f:
                f.write(str(keywords_req.json()))
        else:
            print("keywords_req status code: ", keywords_req.status_code)

    def retrieve_all_data():
        retrieve_all_types()
        retrieve_all_sets()
        retrieve_all_keywords()

    def get_all_sets():
        with open('./data/sets.yaml', 'r') as f:
            sets = load(f, Loader=Loader)
        return sets

    def get_all_types():
        with open('./data/types.yaml', 'r') as f:
            types = load(f, Loader=Loader)
        return types

    def get_all_keywords():
        with open('./data/keywords.yaml', 'r') as f:
            keywords = load(f, Loader=Loader)
        return keywords

    def merge_keywords_data():
        raw = get_all_keywords()
        merged = []
        for category in raw["data"]:
            for keyword in raw["data"][category]:
                if keyword not in merged:
                    merged.append(keyword)
        merged.sort()
        with open('./data/merged_keywords.yaml', 'w') as f:
            f.write(str(merged))

    return
