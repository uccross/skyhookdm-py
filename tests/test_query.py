import unittest
import os 
import sys
import time 

from skyhookdm import query_interface
from skyhookdm.query.models import SQLIR
from skyhookdm.query.engines import SkyhookRunQuery, EngineOptions, DatasetOptions
from skyhookdm.query.parsers import SQLParser

#### Truth values for option validation ####
engine_options_default = {'cls'           : True,
                          'quiet'         : False,
                          'header'        : True,
                          'start-obj'     : '0',
                          'num-objs'      : '2',
                          'output-format' : 'SFT_CSV',
                          'program'       : '~/skyhookdm-ceph/build/bin/run-query'}


engine_options_quiet = {'cls'           : True,
                        'quiet'         : True,
                        'header'        : True,
                        'start-obj'     : '0',
                        'num-objs'      : '2',
                        'output-format' : 'SFT_CSV',
                        'program'       : '~/skyhookdm-ceph/build/bin/run-query'}

engine_options_no_cls = {'cls'           : False,
                        'quiet'         : False,
                        'header'        : True,
                        'start-obj'     : '0',
                        'num-objs'      : '2',
                        'output-format' : 'SFT_CSV',
                        'program'       : '~/skyhookdm-ceph/build/bin/run-query'}

dataset_options_default = {'pool'        : 'tpchdata',
                           'table-name'  : 'lineitem',
                           'oid-prefix'  : 'public'}

### Truth values for skyhook engine commands ###
expected_skyhook_command = ['~/skyhookdm-ceph/build/bin/run-query', 
                            '--num-objs', '2', 
                            '--pool', 'tpchdata', 
                            '--oid-prefix', '"public"', 
                            '--table-name', '"lineitem"', 
                            '--header', 
                            '--use-cls', 
                            '--project "orderkey" ', 
                            '--select "orderkey,lt,5"']


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
    def test_a_sqlir_projection_1(self):
        # Init Query Object
        q = SQLIR()
        eo = query_interface.engine_options(engine='skyhook')
        do = query_interface.dataset_options()

        # Verify default options
        self.assertEqual(eo.options, engine_options_default)
        self.assertEqual(do.options, dataset_options_default)

        q.set_table_name('lineitem')
        q.set_selection('')
        
        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_a_expected.txt")
        self.assertEqual(results, expected)

        time.sleep(5)

    def test_b_sqlir_projection_2_quiet(self):
        # Init Query and options
        q = query_interface("select * from lineitem")
        eo = query_interface.engine_options(engine='skyhook', options={'quiet': True})
        do = query_interface.dataset_options()

        # Verify options use `quiet` setting
        self.assertEqual(eo, engine_options_quiet)
        self.assertEqual(do, dataset_options_default)

        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_b_expected.txt")
        self.assertEqual(results, expected)

        time.sleep(5)
    
    def test_c_sqlir_projection_3_no_cls(self):
        # Init query and options
        q = query_interface.query_string("select orderkey,discount,shipdate from lineitem")
        eo = query_interface.engine_options(engine='skyhook', options={'cls': False})
        do = query_interface.dataset_options()
        
        # Verify options do not use `cls` setting
        self.assertEqual(eo, engine_options_no_cls)
        self.assertEqual(do, dataset_options_default)

        expected_query = {'selection' : [],
                          'projection': ['orderkey,discount,shipdate'], 
                          'table-name': ['lineitem'],
                          'attributes': [],
                          'options'   : []}
        self.assertEqual(q.ir, expected_query)

        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_c_expected.txt")
        self.assertEqual(results, expected)

        time.sleep(5)
    
    #### Selection Queries ####
    def test_d_sqlir_selection_1(self): 
        # Init query and options
        q = query_interface.query_string("select orderkey from lineitem where orderkey > 3")
        eo = query_interface.engine_options()
        do = query_interface.dataset_options()

        # Verify default options
        self.assertEqual(eo, engine_options_default)
        self.assertEqual(do, dataset_options_default)

        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_d_expected.txt")
        self.assertEqual(results, expected)

        time.sleep(5)

    def test_e_sqlir_selection_2_quiet(self):
        # Init query and options
        q = query_interface.query_string("select orderkey,tax,commitdate from lineitem where orderkey < 5")
        eo = query_interface.engine_options(engine='skyhook', options={'quiet': True})
        do = query_interface.dataset_options()
        
        # Verify options do use `quiet` setting
        self.assertEqual(eo, engine_options_quiet)
        self.assertEqual(do, dataset_options_default)

        expected_query = {'selection'  : ['gt', 'orderkey', '3'],
                          'projection' : ['tax,orderkey'], 
                          'table-name' : ['lineitem'],
                          'attributes' : [],
                          'options'    : []}

        self.assertNotEqual(q.ir, expected_query)

        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_e_expected.txt")
        self.assertEqual(results, expected)

        time.sleep(5)
    
    def test_f_sqlir_selection_3_no_cls(self):
        # Init query and options
        q = query_interface.query_string("select orderkey,tax,commitdate from lineitem where orderkey < 5")
        eo = query_interface.engine_options(engine='skyhook', options={'cls': False})
        do = query_interface.dataset_options()
        
        # Verify options do not use `cls` setting
        self.assertEqual(eo, engine_options_no_cls)
        self.assertEqual(do, dataset_options_default)

        results = query_interface.run(q, eo, do)
        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/query/test_f_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    #### Skyhook Engine ####
    def test_g_skyhook_engine(self):
        q = query_interface.query_string("select orderkey from lineitem where orderkey < 5")
        eo = query_interface.engine_options(engine='skyhook')
        do = query_interface.dataset_options()

        command = SkyhookRunQuery.create_sk_cmd(q, eo, do)

        self.assertEqual(command, expected_skyhook_command)

        time.sleep(5)

    #### Parser ####
    def test_h_sqlparser(self):
        parsed = SQLParser.parse_query("select everything from thisTable where everything like 'nothing'")

        expected = get_expected_value(os.getcwd() + "/resources/expected-sql/parser/test_h_expected.txt")
        self.assertEqual(str(parsed), expected) 

        time.sleep(5)

if __name__ == '__main__':
    unittest.main()
