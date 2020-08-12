import unittest
import os 
import sys
import time 

from skyhookdm.query.interfaces import SQLIR
from skyhookdm.query.engines import SkyhookRunQuery
from skyhookdm.query.parsers import SQLParser

#### Options for test validation ####
options_default = {'cls'            : True,
                'quiet'            : False,
                'header'           : True,
                'pool'             : 'tpchdata',
                'num-objs'         : '2',
                'oid-prefix'       : '\"public\"',
                'path_to_run_query': 'cd ~/skyhookdm-ceph/build/ && bin/run-query'}

options_quiet = {'cls'              : True,
                'quiet'            : True,
                'header'           : True,
                'pool'             : 'tpchdata',
                'num-objs'         : '2',
                'oid-prefix'       : '\"public\"',
                'path_to_run_query': 'cd ~/skyhookdm-ceph/build/ && bin/run-query'}

options_no_cls = {'cls'             : False,
                'quiet'            : False,
                'header'           : True,
                'pool'             : 'tpchdata',
                'num-objs'         : '2',
                'oid-prefix'       : '\"public\"',
                'path_to_run_query': 'cd ~/skyhookdm-ceph/build/ && bin/run-query'}


def get_expected_value(path):
    '''
    A function that returns the expected text results from the specified
    file path. 
    '''
    with open(path, 'r') as f:
        data = f.read()
        f.close()
    return data

class TestInterface(unittest.TestCase): 
    '''
    A testing module for each interface provided by the SkyhookSQL client. 
    '''

    # TODO: @Matthew Test query results need to be validated. 
    #### Projection Queries ####
    def test_a_projection_1(self):
        # Init Query Object
        q = SQLIR()

        # Verify default options
        self.assertEqual(q.options, options_default)

        q.sql("select * from lineitem")
        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_a_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    def test_b_projection_2_quiet(self):
        # Init Query
        q = SQLIR() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)

        q.sql("select orderkey,discount,shipdate from lineitem")
        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_b_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    def test_c_projection_3_no_cls(self):
        q = SQLIR()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)

        q.set_projection("orderkey,discount,shipdate")
        q.set_table_name("lineitem")

        expected_query = {'selection'  :'',
                          'projection' :'orderkey,discount,shipdate', 
                          'table-name' :'lineitem'}
        self.assertEqual(q.query, expected_query)

        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_c_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    #### Selection Queries ####
    def test_d_selection_1(self): 
        # Init Query Object
        q = SQLIR()

        # Verify default options
        self.assertEqual(q.options, options_default)

        q.sql("select orderkey from lineitem where orderkey > 3")

        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_d_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    def test_e_selection_2_quiet(self):
        # Init Query
        q = SQLIR() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)

        q.set_projection("orderkey,tax,commitdate")
        q.set_selection("orderkey, lt, 5")
        q.set_table_name("lineitem")

        expected_query = {'selection'  :['gt', 'orderkey', '3'],
                          'projection' :'tax,orderkey', 
                          'table-name' :'lineitem'}

        self.assertNotEqual(q.query, expected_query)

        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_e_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    def test_f_selection_3_no_cls(self):
        q = SQLIR()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)

        q.sql("SELECT * from lineitem where linenumber > 4")

        q.run()

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_f_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    #### Skyhook Runner ####
    def test_g_skyhook_cmd(self):
        q = Query()
        q.set_selection("orderkey, gt, 3")
        q.set_projection("shipdate,orderkey")
        q.set_table_name("lineitem")
        q.set_option("cls", False)
        q.set_option("header", False)
        q.set_option("quiet", True)
        q.set_option("num-objs", 10)
        q.set_option("pool", "name")

        cmd = SkyhookRunner.create_sk_cmd(q.query, q.options)

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/skyhook/test_g_expected.txt")
        self.assertEqual(cmd, expected)

        time.sleep(5)

    #### Parser ####
    def test_h_parse_query(self):
        parsed = SQLParser.parse_query("select everything from thisTable where everything like 'nothing'")

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/parser/test_h_expected.txt")
        self.assertEqual(str(parsed), expected) 

        time.sleep(5)

if __name__ == '__main__':
    unittest.main()
