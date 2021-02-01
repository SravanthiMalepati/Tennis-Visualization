import sqlite3
from sqlite3 import Error

class db(object):
    """
    sqlite3 database connector
    """
    def __init__(self, database):
        """Creating the database connection to a SQLite database"""
        self.conn = None
        try:
            self.conn = sqlite3.connect(database)
            print(sqlite3.version)
        except Error as e:
            print(e)

    def check_row(self, table_name, condition, file_name ):
        c = self.conn.cursor()
        c.execute(''' SELECT count(*) FROM {} WHERE {}='{}' '''.format(table_name, condition, file_name))
        if c.fetchone()[0]>=1 :
            return True
        return False
        
    def create_table(self, create_table_sql):
        """
        Create a table from the create_table_sql statement
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = self.conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)
        
        self.conn.commit()
    
    def insert_data(self, sql, data):
        cur = self.conn.cursor()
        cur.execute(sql, data)
        self.conn.commit()
        return cur.lastrowid

    def update_data(self, sql, match):
        curr = self.conn.cursor()
        curr.execute(sql, match)
        self.conn.commit()
    
    def select_data(self,sql,condition):
        """
        Query tasks by condition
        """
        cur = self.conn.cursor()
        cur.execute(sql, condition)

        rows = cur.fetchall()

        return rows
    def close(self):
        self.conn.close()