from collections import defaultdict
from itertools import count
from mtgsdk import Card

class CalculatedSynergy():
    def __init__(self, id_a, cards):
        self.card_a = Card.find(id_a)
        self.otherCards = cards

    def _calc_colors(self, b_clrs):
        clr_synergy = {
            "score": 0,
            "colors": (self.card_a.colors, b_clrs)
        }
        if len(self.card_a.colors) > 0 and len(b_clrs) > 0:
            for color in self.card_a.colors:
                if color in b_clrs:
                    clr_synergy["score"] += 1
        else:
            clr_synergy["score"] = 1
        return clr_synergy

    def _calc_keyword_abilities(self, b_card):
        abil_synergy = 0
        try:
            print("$$$A ->", self.card_a.text)
        except:
            print("--- A has NO TEXT ---")
        try:
            print("$$$B ->", b_card["text"])
        except:
            print("--- B has NO TEXT ---")
        finally:
            return abil_synergy

    def create_synergy_report(self, b_card):
        report = defaultdict()
        if self.card_a.name == b_card["name"]:
            return {
                "synergy_score": 100,
                "name": self.card_a.name
            }
        else:
            report["evaluatedCards"] = {"selectedCard": self.card_a.name, "compCard": b_card["name"]}
            report["colorSynergy"] = dict(self._calc_colors(b_card["colors"]))
            report["keywordAbilities"] = self._calc_keyword_abilities(b_card)
            report["synergyScore"] = report["colorSynergy"]["score"] + report["keywordAbilities"]
            return report

    def get_synergy_scores(self): 
        scores = defaultdict()
        for card in self.otherCards:
            report = self.create_synergy_report(card)
            scores[card['name']] = report
        synergyScore = {"cardID": self.card_a.multiverse_id, "cardName": self.card_a.name, "synergyScores": scores}
        return synergyScore
