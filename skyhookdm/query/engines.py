import os
import subprocess

from .models import SQLIR

class SkyhookRunQuery:
    """An engine for building and executing Skyhook CLI commands."""

    @classmethod
    def create_sk_cmd(cls, query, engine_options, dataset_options):
        """A function that yields a generator for the Skyhook CLI command from a Query Object. 

        Arguments:
        query   -- A Query dictionary  
        options -- A dictionary of Skyhook options

        Returns:
        skyhook_cmd -- A list of the arguments of the Skyhook run-query command stirng
        """
        if not isinstance(query, SQLIR):
            raise TypeError("Query must be of type SQLIR")
        if not isinstance(engine_options, VerifiedOptions):
            raise TypeError("Engine options must be of type EngineOptions")
        if not isinstance(dataset_options, VerifiedOptions):
            raise TypeError("Dataset options must be of type DatasetOptions")

        command_args = [
            engine_options.options['program'],
            
            '--num-objs'   , engine_options.options['num-objs'],
            '--pool'       , dataset_options.options['pool'],
            '--oid-prefix' , f"\"{dataset_options.options['oid-prefix']}\"",
            '--table-name' , f"\"{query.ir['table-name']}\""
        ]

        if engine_options.options['header']:
            command_args.append("--header")

        if engine_options.options['cls']:
            command_args.append("--use-cls")

        if engine_options.options['quiet']:
            command_args.append("--quiet")

        if query.ir['projection']:
            projection = ','.join(query.ir['projection']).replace(' ', '')
            command_args.append("--project \"{}\" ".format(projection))

        if query.ir['selection']:
            predicates = ';'.join(query.ir['selection']).replace(' ', '')
            command_args.append("--select \"{}\"".format(predicates))

        return command_args

    @staticmethod
    def _execute_sk_cmd(cls, command_args):
        """A function that executes a Skyhook CLI command. 

        Arguments:
        command_args -- A list of arguments to be executed in which the first must be a path to Skyhook's run-query binary

        Returns: The stdout results of a subprocess execution of command_args 
        """
        cmd_completion = subprocess.run(command_args, check=True, stdout=subprocess.PIPE)
        return cmd_completion.stdout

    @classmethod
    def run_query(cls, query, engine_options, dataset_options): 
        """A function that generates and executes a Skyhook CLI command from a query object. 

        Arguments: 
        query   -- A Query dictionary
        options -- A dictionary of Skyhook options 

        Returns: The results of a query execution
        """
        command_args = cls.create_sk_cmd(query, engine_options, dataset_options)

        return cls._execute_sk_cmd(command_args)

class DatasetOptions():
    """A class that manages the metadata of a dataset"""
    
    all_dataset_options = ['pool', 'table_name', 'oid_prefix']

    defaults = {'pool'        : 'tpchdata',
                'table-name'  : 'lineitem',
                'oid-prefix'  : 'public'}
    
    @classmethod
    def get_dataset(cls, dataset=None):
        """A function that returns dataset metadata

        Arguments:
        dataset -- A string representing the name of a dataset
        """
        if dataset is None:
            return VerifiedOptions(cls.defaults)
        if not isinstance(dataset, str):
            raise TypeError("Input must be a string")
        else:
            cls._find_dataset(dataset)
            # TODO: @Matthew How to get dataset, and verify? 
        
    @staticmethod
    def _find_dataset(dataset):
        pass
        

class EngineOptions():
    """A class that manages the SkyhookRunQuery engine options"""

    all_options = ['cls', 'quiet', 'start_obj',
                   'num_objs', 'output_format', 'program']

    skyhook_defaults = {'cls'           : True,
                        'quiet'         : False,
                        'header'      : True,
                        'start-obj'     : 0,
                        'num-objs'      : 2,
                        'output-format' : 'SFT_CSV',
                        'program'       : 'bin/run-query'}

    @classmethod
    def get_options(cls, options=None):
        """A function that returns Skyhook Engine options 

        Arguments:
        options -- If None, return default options. Otherwise, check options and return
                   dictionary of Skyhook options
        """
        if options is None:
            return VerifiedOptions(cls.skyhook_defaults)

        return VerifiedOptions(cls._set_options(options, cls.all_options))
        

    @staticmethod
    def _set_options(options, all_options):
        """Sets the option to be the given value.

        Arguments:
        option -- The dictionary of options and their values
        all_options -- The list of available options 
        """
        if not EngineOptions._check(options, all_options):
            raise ValueError(f"{option} not an available option")
        return options

    @staticmethod
    def _check(options, all_options):
        """Checks that the options are available for Skyhook

        Arguments:
        options -- A dictionary of options and their values
        all_options -- The list of available options
        """
        for option in options:
            if option not in all_options:
                return False
        return True

class VerifiedOptions():
    """A dummy class that simply confirms that options were checked"""
    def __init__(self, options):
        self.options = options
