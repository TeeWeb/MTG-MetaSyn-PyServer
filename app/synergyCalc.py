from collections import defaultdict, namedtuple
from itertools import count
from mtgsdk import Card

class CalculatedSynergy():
    def __init__(self, id_a, card):
        self.selected_card = Card.find(id_a)
        self.comp_card = card
        self.scores = {"synergy_score": None, "evaluated_cards": None, "color_synergy": {}, "keyword_abilities": None}

    def _calc_colors(self):
        clr_synergy = {
            "score": 0,
            "colors": (self.selected_card.colors, self.comp_card["colors"])
        }
        if self.selected_card.colors is None or self.comp_card["colors"] is None:
            clr_synergy["score"] += 1
        elif len(self.selected_card.colors) > 0 and len(self.comp_card["colors"]) > 0:
            for color in self.selected_card.colors:
                if color in self.comp_card["colors"]:
                    clr_synergy["score"] += 1
        else:
            clr_synergy["score"] = 1
        return clr_synergy

    def _calc_keyword_abilities(self, b_card):
        abil_synergy = 0
        try:
            print("$$$A ->", self.selected_card.text)
        except:
            print("--- A has NO TEXT ---")
        try:
            print("$$$B ->", b_card["text"])
        except:
            print("--- B has NO TEXT ---")
        finally:
            return abil_synergy

    def get_synergy_scores(self):
        if self.selected_card.name == self.comp_card["name"]:
            return {
                "synergy_score": 100,
                "name": self.selected_card.name
            }
        else:
            self.scores["evaluated_cards"] = {"selectedCard": self.selected_card.name, "compCard": self.comp_card['name']}
            self.scores["color_synergy"] = dict(self._calc_colors())
            self.scores["keyword_abilities"] = self._calc_keyword_abilities(self.comp_card)
            self.scores["synergy_score"] = self.scores["color_synergy"]["score"] + self.scores["keyword_abilities"]
        return self.scores
