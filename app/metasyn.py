from collections import namedtuple
from itertools import combinations

from mtgsdk import Card

class Utils():
  
  def get_color_synergies(self, colors_list):
    print(colors_list)
    mtg_colors = ('B', 'G', 'R', 'U', 'W')
    normalized_mtg_colors = "".join(mtg_colors)
    normalized_color_identity = "".join(colors_list)
    color_combos = {}
    two_clr_combos = combinations(normalized_mtg_colors, 2)
    three_clr_combos = combinations(normalized_mtg_colors, 3)
    four_clr_combos = combinations(normalized_mtg_colors, 4)
    
    for i in mtg_colors: color_combos[i] = False
    for j in list(two_clr_combos): color_combos["".join(j)] = False
    for k in list(three_clr_combos): color_combos["".join(k)] = False
    for l in list(four_clr_combos): color_combos["".join(l)] = False
    color_combos[normalized_mtg_colors] = False
    for combo in color_combos:
      # Check if card is colorless. If so, card synergizes with all color combos
      if colors_list is None:
        color_combos[combo] = True
      # Mark matching color combos as True if the card's color identity is found to match at least a subset of colors in a color combo
      # Example 1: "B" matches "B" & "BW" color combos
      # Example 2: "UW" does NOT match "U" or "W", but does match "UW" and "URW"
      elif normalized_color_identity in combo:
        color_combos[combo] = True
    
    return color_combos
