from elasticsearch import Elasticsearch
import logging

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host':'localhost', 'port':9200}])
    if _es.ping():
        print('Connected')
    else:
        print('Cannot connect')
    
    return _es

def close_connection(es):
    if es:
        es.transport.connection_pool.close()
        print('Connection closed')

def create_index(es, index_name, settings):
    created = False
    try:
        if not es.indices.exists(index_name):
            es.indices.create(index = index_name, ignore=400, body = settings)
            print('Created Index')
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def store_record(es, index_name, doc_type, record):
    try:
        outcome = es.index(index=index_name, doc_type=doc_type, body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
    return outcome

def delete_index(es, index):
    output = es.indices.delete(index=index, ignore=[400, 404])
    return output

def search(es, index_name, search):
    res = es.search(index=index_name, body = search)
    return res

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)