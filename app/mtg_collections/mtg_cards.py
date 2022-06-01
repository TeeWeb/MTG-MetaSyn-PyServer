import requests
import json
import os

from app.mtg_collections.update import IUpdater

class CardsUpdater(IUpdater):
    _capitalized_name = "Cards"
    _data_endpoint = "https://mtgjson.com/api/v5/AtomicCards.json"
    _identifier = "scryfallOracleId"
    new_items = []
    new_attributes = []

    def local_update_needed(self) -> bool:
        return self.__is_local_update_needed(self.last_updated())

    def get_distinct_coll_items(self) -> list:
        return self.get_whole_collection().distinct("identifiers")

    def get_items_to_add(self) -> list:
        self.new_items = []
        local_data = self.local.get_data()
        coll_items = self.get_distinct_coll_items()
        for cached_item in local_data:
            print(cached_item)
            if cached_item not in coll_items:
                self.new_items.append({ self._identifier: cached_item })
        return self.new_items

    def get_items_to_update(self) -> list:
        self.new_attributes = []
        print("TODO: implement card attributes updater")
        return self.new_attributes

    # TODO: create function to retrieve and update AllCards collection in DB
    def handle_cards_update(self):
        print("--- Cards ---")
        # Check release date of current card data
        if self.local.__is_outdated():
            # Get latest sets from newAtomicCards.json
            # TODO: Use 'newAtomicCards.json' that has already been featched rather than requesting again
            new_data = requests.get(
                self._data_endpoint).json()
            last_data_update = new_data['meta']['date']
            cards = {"cards": []}
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
                    card = {str(card_version['identifiers']['scryfallOracleId']): card_version }
                    cards['cards'].append(card)
            cards_data = {"meta": {"date": last_data_update}, "cards": cards}
            with open(self._data_dir_path + 'AtomicCards.json', 'w') as f:
                f.write(json.dumps(cards_data, indent=4))
            # Compare most recent list of cards with DB Collection
            collection = self.get_db_collection('cards')
            # TODO: add function to run synergy calculator on new cards BEFORE they're inserted into database
            # Get a list of dicts{'scryfallOracleId': <id>} that are not already in the DB Collection
            new_oracle_ids = self.get_items_to_add(self, collection, cards,
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
            os.remove(self._data_dir_path + 'newAtomicCards.json')
        except Exception as e:
            print("Unable to remove temp data file",
                              e,
                              exc_info=True)
        return

    def update_local_data(self):
        print("Updating Cards data...")
        self.local.update()
