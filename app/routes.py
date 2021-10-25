from flask import request, jsonify
from app import app, db
from app.synergyCalc import CalculatedSynergy


@app.route('/api', methods=['GET'])
def api():
    return {"data": 1234}


@app.route('/api/sets', methods=['GET'])
def sets():
    sets_cursor = db.sets.find({}, {"_id": 0, "code": 1}).sort("code")
    sets = []
    for set in list(sets_cursor):
        sets.append(set['code'])
    return jsonify(sets)


@app.route('/api/keywords', methods=['GET'])
def keywords():
    keywords_cursor = db.keywords.find(
        {}, {"_id": 0, "keyword": 1}).sort("keyword")
    keywords = []
    for keyword in list(keywords_cursor):
        keywords.append(keyword['keyword'])
    return jsonify(keywords)


@app.route('/api/types', methods=['GET'])
def types():
    card_types_cursor = list(db.types.find(
        {}, {"_id": 0, "type": 1}).sort("type"))
    card_types = []
    for card_type in card_types_cursor:
        card_types.append(card_type['type'])
    return jsonify(card_types)


@app.route('/api/subtypes', methods=['GET'])
def subtypes():
    print(request.args.get('type'))
    if not request.args.get('type') or request.args.get('type') == "undefined":
        return jsonify(["Select a Type"])
    else:
        selected_type = request.args.get('type')
        subtypes_dict = db.types.find({"type": selected_type}, {
            "_id": 0, "subtypes": 1}).sort("subtypes").next()
        subtypes = subtypes_dict['subtypes']
        return jsonify(subtypes)


@app.route('/api/gatherCards', methods=['POST'])
def gatherCards():
    data = request.get_json()
    cardsCursor = db.AllCards.find({'setCode': data['setCode']}, {'_id': 0, 'name': 1, 'type': 1, 'types': 1, 'subtypes': 1, 'power': 1,
                                   'toughness': 1, 'multiverseId': 1, 'colors': 1, 'colorIdentity': 1, 'cmc': 1, 'setCode': 1, 'keywords': 1, 'text': 1})
    cards = []
    for card in list(cardsCursor):
        cards.append(card)
    return jsonify(cards)


@app.route('/api/synergize', methods=['POST'])
def synergize():
    selected_card = request.args.get('card')
    data = request.get_json()
    synergy = CalculatedSynergy(selected_card, data['otherCards'])

    syn_calc = synergy.get_synergy_scores()

    return jsonify(syn_calc)
