import es

if __name__ == '__main__':
    es_conn = es.connect_elasticsearch()
    output = es.delete_index(es_conn, 'matches')
    es.close_connection(es_conn)
    print(output)