import pandas as pd

from datetime import datetime, timedelta
from collections import OrderedDict
from .db import db
from src import es
import json

def create_database():
    """
    Initialize a database and create the table if not present and return True
    """
    global conn
    conn = db('./data/db/matches.db')
    conn.create_table(create_match_sql())

def create_match_sql():
    return """CREATE TABLE IF NOT EXISTS matches (
                Tournament text NOT NULL,
                Date text NOT NULL,
                Round text NOT NULL,
                Player_1 text NOT NULL,
                Player_2 text NOT NULL,
                file_name text NOT NULL,
                index_date text NOT NULL,
                won text NOT NULL,
                result text NOT NULL,
                status text NOT NULL,
                url text
            );"""

def tournament_sql():
    return "SELECT * FROM matches WHERE status=?"

def matches_sql():
    return "SELECT * FROM matches WHERE Tournament=? AND status=?"


def get_past_date(days = 0):
    format = '%Y-%m-%d'
    start = datetime.today() - timedelta(days = days)
    return datetime.strptime(start.strftime(format), format)
    
def get_matches_list(df, size = True):
    length = df.shape[0]
    if size and length > 5:
        df = df.iloc[:5]
    
    results = df.to_dict('records', into=OrderedDict)
    if size:
        results.append(length)

    return results

def get_tournaments(live):

    results = dict()
    create_database()
    
    if live:
        tournaments = conn.select_data(tournament_sql(), ('live',))
    elif not live:
        tournaments = conn.select_data(tournament_sql(), ('finished',))

    tournaments = pd.DataFrame(tournaments, columns = ['Tournament','Date','Round','Player_1','Player_2','file_name','index_date','won','result','status','url'])
    tourn_list = tournaments['Tournament'].value_counts().index.to_list()
    for tournament in tourn_list:
        results[tournament] = get_matches_list(tournaments[tournaments['Tournament'] == tournament])
    conn.close()
    return results

def get_matches(tournament, live, **kwargs):

    results = dict()
    create_database()

    if live:
        tournaments = conn.select_data(matches_sql(), (tournament, 'live'))
    elif not live:
        tournaments = conn.select_data(matches_sql(), (tournament, 'finished'))
    tournaments = pd.DataFrame(tournaments, columns = ['Tournament','Date','Round','Player_1','Player_2','file_name','index_date','won','result','status','url'])
    results[tournament] = get_matches_list(tournaments[tournaments['Tournament'] == tournament], False)
    
    return results

def get_search_results(query, field):
    es_conn = es.connect_elasticsearch()
    body = None
    matches = list()
    if field == 'round':
        body = { "query": {
            "multi_match" : {
                "query":    query, 
                "fields": [ "round" ] 
            }
            }
        }
    elif field == 'player':
        body = { "query": {
            "multi_match" : {
                "query":    query, 
                "fields": [ "Player_1", "Player_2" ] 
            }
            }
        }
    elif field == 'date':
        body = { "query": {
            "multi_match" : {
                "query":    query, 
                "fields": [ "Date" ] 
            }
            }
        }
    elif field == 'tournament':
        body = { "query": {
            "multi_match" : {
                "query":    query, 
                "fields": [ "tournament" ] 
            }
            }
        }
    
    if body:
        results = es.search(es_conn, 'matches', body )
        for i in results['hits']['hits']:
            matches.append(i['_source'])
        es.close_connection(es_conn)
    print(matches)
    to_return = {query: matches}
    return to_return
    
