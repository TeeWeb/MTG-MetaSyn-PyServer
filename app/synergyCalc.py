from collections import namedtuple
from itertools import count
from mtgsdk import Card

synergy_report_template = {
    "synergy_score": None,
    "cards": None,
    "colors": {},
    "keyword_abilities": None
}

class CalculatedSynergy():
    def __init__(self, id_a, cards):
        self.card_a = Card.find(id_a)
        self.otherCards = cards

    def _calc_colors(self, b_clrs):
        clr_synergy = {
            "score": 0,
            "colors": (self.card_a.colors, b_clrs)
        }
        print(self.card_a.colors, b_clrs)
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
        report = synergy_report_template
        if self.card_a.name == b_card["name"]:
            return {
                "synergy_score": 100,
                "name": self.card_a.name
            }
        else:
            report["cards"] = [self.card_a.name, b_card["name"]]
            report["colors"] = dict(self._calc_colors(b_card["colors"]))
            report["keyword_abilities"] = self._calc_keyword_abilities(b_card)
            report["synergy_score"] = report["colors"]["score"] + report["keyword_abilities"]
            return report

    def get_synergy_scores(self): 
        SynergyScores = namedtuple('SynergyScores', 'cardId cardName relatedCards')
        reports = []
        for card in self.otherCards:
            report = self.create_synergy_report(card)
            reports.append(dict(report))
        synergyScore = SynergyScores(self.card_a.multiverse_id, self.card_a.name, reports)
        return synergyScore
