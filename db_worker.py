import sqlite3
import requests
from configparser import ConfigParser
from kafka_reader import load_config


def connect_db():
    return sqlite3.connect('product_db.db')


def close_db(con):
    con.close()


def initialize_db():
    con = sqlite3.connect('product_db.db')
    cur = con.cursor()
    if check_if_tables_exist(cur) is False:
        cur.execute(
            '''CREATE TABLE "Offers" ("id" text NOT NULL,"category"	text, \
                "name" text, "description" text, PRIMARY KEY("id"))''')
        cur.execute(
            '''CREATE TABLE "Categories" ("name" text NOT NULL,	\
                "parent_category" text,	PRIMARY KEY("name"))''')
        cur.execute(
            '''CREATE TABLE "Parameters" ("id"	TEXT NOT NULL, \
                "key" text NOT NULL, "value"	text, PRIMARY KEY("id","key"))''')
        cur.execute(
            '''CREATE TABLE "Similarities" ("id1"	TEXT NOT NULL, \
                "id2" TEXT NOT NULL, "nr_of_same_params"	INTEGER, \
                    "nr_of_diffe_params" INTEGER, PRIMARY KEY("id1","id2"))''')
    close_db(con)


def check_if_tables_exist(cur):
    try:
        cur.execute('''SELECT id from Offers limit 1''')
        if cur.fetchone()[0] != 1:
            return False
        cur.execute('''SELECT name from Categories limit 1''')
        if cur.fetchone()[0] != 1:
            return False
        cur.execute('''SELECT id from Parameters limit 1''')
        if cur.fetchone()[0] != 1:
            return False
        cur.execute('''SELECT id from Similarities limit 1''')
        if cur.fetchone()[0] != 1:
            return False
        return True
    except Exception as err:
        print('Missing tables in db')
        return False


def save_offer(offer):
    con = connect_db()
    cur = con.cursor()
    # saving the offer itself
    cur.execute(
        '''INSERT INTO Offers values(:id, :category, :name, :description) \
            on conflict(id) do update set category = excluded.category, \
            name = excluded.name, description = excluded.description''', offer)
    # using the offer id to save the parameters in second table
    for (key, val) in offer['parameters'].items():
        cur.execute('''INSERT into Parameters values(?, ?, ?) \
                on conflict(id, key) do update set value = excluded.value''',
                    (offer['id'], key, val))
    # getting similar offers from the api
    same_offers = get_same_prod_offers(offer['id'])
    if 'matching_offers' in same_offers:
        for offer_id in same_offers['matching_offers']:
            # the nr of same params done by counting nr of
            # matching keys for 2 different keys
            same_param_cnt = count_same_params(offer['id'], offer_id)
            # nr of different params can be calculated as nr of distinct params
            # between the 2 ids minus the differing params
            diff_param_cnt = count_distinct_params(
                offer['id'], offer_id) - same_param_cnt
            cur.execute(
                '''INSERT into Similarities values(?, ?, ?, ?) \
                on conflict(id1, id2) do update\
                set nr_of_same_params = excluded.nr_of_same_params,\
                nr_of_diffe_params = excluded.nr_of_diffe_params''',
                (offer['id'], offer_id, same_param_cnt, diff_param_cnt))
    con.commit()
    con.close()


def get_same_prod_offers(offer_id):
    config = load_config('API')

    custom_header = {'Auth': config['auth_token']}
    response = requests.get(
        "{}}/{}".format(config['api_url'], offer_id),
        headers=custom_header)
    if response.status_code == 200:
        return response.json()
    else:
        return []


def count_same_params(id1, id2):
    con = connect_db()
    cur = con.cursor()
    # if the ids are same, then nr of same params is the number of params
    #  for either of them
    if id1 == id2:
        cur.execute('''select count( p1.key ) \
        from Parameters as p1 \
             where p1.id = ?''', [id1])
    else:
        cur.execute('''select count(*) \
            from Parameters as p1 \
                inner join Parameters as p2 on p1.id <> p2.id \
                    and p1.key == p2.key\
                 where p1.id = ? and p2.id = ?''', (id1, id2))
    retval = cur.fetchone()[0]
    con.close()
    return retval


def count_distinct_params(id1, id2):
    con = connect_db()
    cur = con.cursor()
    cur.execute('''select count(distinct p1.key ) \
        from Parameters as p1 \
             where p1.id = ? or p1.id = ?''', (id1, id2))
    retval = cur.fetchone()[0]
    con.close()
    return retval


def save_category(category):
    if 'name' not in category:
        return
    if category['name'] == '':
        return
    con = connect_db()
    cur = con.cursor()

    if 'parent_category' not in category:
        category['parent_category'] = ''

    cur.execute(
        '''INSERT INTO Categories values(?, ?) on conflict(name)\
        do update set parent_category = excluded.parent_category''',
        (category['name'], category['parent_category']))
    con.commit()
    con.close()
