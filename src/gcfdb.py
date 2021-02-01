import sqlite3
import pandas as pd

def main():
    conn = sqlite3.connect('./data/db/matches.db')
    cur = conn.cursor()
    cur.execute("select * from matches")
    rows = cur.fetchall()
    matches = pd.DataFrame(rows, columns = ['Tournament','Date','Round','Player_1','Player_2','file_name','index_date','won','result','status','url'])   
    matches.to_csv('./data/matches.csv', index = False)

if __name__ =='__main__':
    main()