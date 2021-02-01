from flask import render_template, url_for, request
from app import app
from src import utils

@app.route("/")
@app.route("/index")
def index():
    return render_template('index.html')

@app.route("/live_tournaments")
def live_tournaments():
    tournaments = utils.get_tournaments(live = True)
    return render_template("tournament.html", title = 'Live Tournaments', tournaments = tournaments, matches_url = 'live_matches')

@app.route("/finished_tournaments")
def finished_tournaments():
    tournaments = utils.get_tournaments(live = False)
    return render_template("tournament.html", title = 'Finished Tournaments', tournaments = tournaments, matches_url = 'finished_matches')

@app.route("/matches/<filename>", methods = ['GET'])
def matches(filename):
    return render_template('match.html', filename = filename, title = 'Tennis Visualization')

@app.route("/live_matches/<tournament>")
def live_matches(tournament):
    matches_list = utils.get_matches(tournament, live = True)
    return render_template("matches_list.html", matches = matches_list, title = tournament)

@app.route("/finished_matches/<tournament>")
def finished_matches(tournament):
    matches_list = utils.get_matches(tournament, live = False)
    return render_template("matches_list.html", matches = matches_list, title = tournament)

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        field = request.form.get('fields')
        query = request.form.get('query')
        matches_list = utils.get_search_results(query, field)
        return render_template("search_results.html", matches = matches_list, title = query)
    return render_template("index.html")
