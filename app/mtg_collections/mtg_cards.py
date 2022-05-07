import requests
import json
import os

from app.mtg_collections.update import IUpdater

class CardsUpdater(IUpdater):
    _capitalized_name = "Cards"
    _collection_name = "cards"
    _data_endpoint = "https://mtgjson.com/api/v5/AtomicCards.json"

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

    # TODO: create function to retrieve and update AllCards collection in DB
    def handle_cards_update(self):
        print("--- Cards ---")
        # Check release date of current card data
        if self.local_update_needed(
                self, self.file_name,
                self._data_endpoint):
            # Get latest sets from newAtomicCards.json
            # TODO: Use 'newAtomicCards.json' that has already been featched rather than requesting again
            new_data = requests.get(
                self._data_endpoint).json()
            last_data_update = new_data['meta']['date']
            cards = []
            oracle_ids = {"cards": []}
            # AtomicCards data contains objects where:
            # KEY is a card name (key=<card_name>)
            # VALUE is an array of objects representing versions of cards with that name
            # Build a list of card versions and create AtomicCards.json file
            for card_name in new_data['data']:
                for card_version in new_data['data'][card_name]:
                    # Create a hash of card's faceName + scryfallOracleId to create a unique "_id" value
                    try:
                        card_version['_id'] = hash(
                            (card_version['identifiers']['scryfallOracleId'],
                             card_version['faceName']))
                    except:
                        card_version['_id'] = hash(
                            card_version['identifiers']['scryfallOracleId'])
                    # Remove foreignData and printings from cardData to reduce size
                    try:
                        del card_version['foreignData']
                    except Exception as e:
                        print("Exception:", e)
                    try:
                        del card_version['printings']
                    except Exception as e:
                        print("Exception:", e)
                    cards.append(card_version)
                    oracle_ids['cards'].append(
                        str(card_version['identifiers']['scryfallOracleId']))
            cards_data = {"meta": {"date": last_data_update}, "cards": cards}
            with open(self.data_dir_path + 'AtomicCards.json', 'w') as f:
                f.write(json.dumps(cards_data, indent=4))
            # Compare most recent list of cards with DB Collection
            collection = self.get_db_collection('cards')
            # TODO: add function to run synergy calculator on new cards BEFORE they're inserted into database
            # Get a list of dicts{'scryfallOracleId': <id>} that are not already in the DB Collection
            new_oracle_ids = self.get_new_items(self, collection, oracle_ids,
                                               'cards')
            if len(new_oracle_ids) > 0:
                # Find the corresponding card object for each scryfallOracleId in new_oracle_ids
                new_items = []
                print(new_oracle_ids)
                new_ids = []
                for oid in new_oracle_ids:
                    new_ids.append(oid['scryfallOracleId'])
                for card in cards_data['cards']:
                    if card['identifiers']['scryfallOracleId'] in new_ids:
                        # Once set object is found within cards_data, add it to the list of items to insert into DB
                        new_items.append(card)
                        print("Prepping %s to add to DB: %s" %
                              (card['identifiers']['scryfallOracleId'],
                               card['_id']))
                # Insert all new set objects into the DB
                self.insert_new_items(self, collection, new_items)
            else:
                print("No new cards")
        # Remove temp newSets.json file now that sets.json file has been updated
        try:
            os.remove(self.data_dir_path + 'newAtomicCards.json')
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
        print("Updating Cards data...")
        pass