from db_worker import count_distinct_params, save_category, count_same_params, connect_db
import unittest


class Test_db_methods(unittest.TestCase):

    def test_save_category(self):
        con = connect_db()
        cur = con.cursor()
        test_category = {
            'name': 'test_category', 'parent_category': 'test_parent'}
        test_no_parent = {'name': 'test_no_parent'}
        test_no_name = {'name': ''}

        save_category(test_category)
        cur.execute(
            '''select count(*) from Categories where name = "test_category"''')
        self.assertEqual(cur.fetchone()[0], 1, 'FAILED: Save category basic')
        save_category(test_no_parent)
        cur.execute(
            '''select count(*) from Categories where name = "test_no_parent"'''
            )
        self.assertEqual(
            cur.fetchone()[0], 1, 'FAILED: Save category no parent')
        save_category(test_no_name)
        cur.execute('''select count(*) from Categories where name = ""''')
        self.assertEqual(
            cur.fetchone()[0], 0, 'FAILED: Save category no name failed')
        # cleanup db after testing
        cur.execute(
            '''delete from Categories where name = "test_no_parent" or \
                name = "test_category" or name = ""''')
        con.commit()
        con.close()

    def test_save_offer(self):
        # cannot really test without having the api running,
        # maybe some more complex test with setting my own api could work
        pass

    def test_offer_params(self):
        con = connect_db()
        cur = con.cursor()
        cur.execute(
            '''INSERT INTO Parameters values\
                ("test_id_1", "test_key_1", "empty value"),\
                ("test_id_1", "test_key_2", "empty value"),\
                ("test_id_1", "test_key_3", "empty value"),\
                ("test_id_2", "test_key_1", "empty value"),\
                ("test_id_2", "test_key_2", "empty value"), \
                ("test_id_2", "test_key_4", "empty value")'''
                                    )
        con.commit()
        same_params = count_same_params('test_id_1', 'test_id_2')
        distinct_params = count_distinct_params('test_id_1', 'test_id_2')
        self.assertEqual(same_params, 2, 'Same parameters test')
        self.assertEqual(distinct_params, 4, 'Distinct params test')
        cur.execute('''
        delete from Parameters where id="test_id_1" or id="test_id_2"
        ''')
        con.commit()
        con.close()


if __name__ == '__main__':
    unittest.main()
