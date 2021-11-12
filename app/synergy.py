from collections import defaultdict, namedtuple
from itertools import count
from mtgsdk import Card

from app.metasyn import Utils

class CardSynergy():
    def __init__(self, card_id):
        self.selected_card = Card.find(card_id)
        self.scores = {
            "color_synergies": Utils().get_color_synergies(self.selected_card.color_identity), 
            "keyword_synergies": None
        }

    def _calc_relative_color_syn(self, card_b_colors):
        ###
        # Tallies a score integer by comparing color_synergies of the selected card with another's
        # via the Utils().get_color_synergies() utility function
        # Colorless cards synergize with all colors
        ###
        clr_synergy = 0
        card_b_synergies = Utils().get_color_synergies(card_b_colors)
        print(card_b_synergies)
        for clr_combo in card_b_synergies:
            if clr_combo and self.scores['color_synergies'][clr_combo]:
                clr_synergy += 1
        return clr_synergy

    def _calc_keyword_abilities(self, card_b):
        ###
        # 
        ###
        abil_synergy = 0
        try:
            print("$$$A ->", self.selected_card.text)
        except:
            print("--- A has NO TEXT ---")
        try:
            print("$$$B ->", card_b["text"])
        except:
            print("--- B has NO TEXT ---")
        finally:
            return abil_synergy

    def get_relative_synergy_scores(self, comp_card):
        ###
        # Analyzes selected card's synergy in relation to one other card and returns a RelativeSynergy object with the relative synergy scores
        ###
        print(comp_card)
        RelativeSynergy = namedtuple('RelativeSynergy', ["evaluated_cards", "color_synergy", "keyword_synergy", "overall"])
        color_syn = self._calc_relative_color_syn(comp_card['colors'])
        keyword_syn = self._calc_keyword_abilities(comp_card)
        overall = color_syn + keyword_syn
        
        synergy_scores = RelativeSynergy({"selectedCard": self.selected_card.name, "compCard": comp_card['name']}, 
            color_syn,
            keyword_syn,
            overall
        )
        return synergy_scores
