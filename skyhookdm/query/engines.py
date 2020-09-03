import os
import subprocess
from abc import ABC, abstractmethod

from .models import SQLIR

class Engine(ABC):
    """Abstract class for engine implementations.

    Engines (e.g. Skyhook's Run Query) are used to transform
    an intermediate representation of queries into commands or
    library calls to a particular data management system. This
    class provides a skeleton for functionality that must be 
    supported by any engine implemented via this interface. 
    """
    def __init__(self, name, ):
        pass

class SkyhookRunQuery(Engine):
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
        if not isinstance(engine_options, Options):
            raise TypeError("Engine options must be of type Options")
        if not isinstance(dataset_options, Options):
            raise TypeError("Dataset options must be of type Options")

        command_args = [
            engine_options.options['program'],
            
            '--num-objs'   , engine_options.options['num-objs'],
            '--pool'       , dataset_options.options['pool'],
            '--oid-prefix' , f"\"{dataset_options.options['oid-prefix']}\"",
            '--table-name' 
        ]

        if len(query.ir['table-name']) > 0:
            command_args.append(f"\"{','.join(query.ir['table-name']).replace(' ', '')}\"")
        else:
            command_args.append(f"\"{(dataset_options.options['table-name']).replace(' ', '')}\"")

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

        if 'limit' in query.ir['options']:
            command_args.append("--limit 0")
        
        # TODO: @Matthew Need a new way to represent attributes of a table in a create index 
        # query, and still know what table to use 
        if 'index-create' in query.ir['options']:
            command_args.append("--index-create")        

        if 'index-cols' in query.ir['options']:
            command_args.append("--index-cols")
            attributes = ','.join(query.ir['attributes']).replace(' ','')
            command_args.append(f"{attributes}")


        return command_args

    @staticmethod
    def _execute_sk_cmd(command_args):
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

class Options():
    """(Dummy) class for engine and dataset options

    TODO: @Matthew
    Engines and datasets that use options to represent argument flags 
    or metadata. This abstract class implements a general procedure
    for verifying that options are allowed by an engine or dataset. 
    """
    def __init__(self, opts):
        self.options = opts

class DatasetOptions():
    """A class that manages the metadata of a dataset"""
    
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
            return Options(cls.defaults)
        if not isinstance(dataset, str):
            raise TypeError("Input must be a string")
        else:
            cls._find_dataset(dataset)
            # TODO: @Matthew How to get dataset, and verify? 
        
    @staticmethod
    def _find_dataset(dataset):
        all_dataset_options = ('pool', 'table_name', 'oid_prefix')
        # TODO: @Matthew Calls skyhook_get_dataset() method
        pass
        

class EngineOptions():
    """A class that manages the SkyhookRunQuery engine options"""
    skyhook_defaults = {'cls'           : True,
                        'quiet'         : False,
                        'header'        : True,
                        'start-obj'     : '0',
                        'num-objs'      : '2',
                        'output-format' : 'SFT_CSV',
                        'program'       : '~/skyhookdm-ceph/build/bin/run-query'}

    engines = {'skyhook': skyhook_defaults}

    @classmethod
    def get_options(cls, engine=None, options=None):
        """A function that returns Skyhook Engine options 

        Arguments:
        options -- If None, return default options. Otherwise, check options and return
                   dictionary of Skyhook options
        """
        if engine is None and options is None:
            return Options(cls.skyhook_defaults)
        elif options is None:
            return cls._get_def_options(engine)
        else:
            return cls._set_def_options(engine, options)

        return cls._set_options(options)
        
    @staticmethod
    def _get_def_options(engine):
        """Gets default options for the specified engine

        Arguments
        engine -- A string representing the name of an engine
        """
        if engine in EngineOptions.engines:
            return EngineOptions.engines[engine]
        else:
            raise ValueError(f"{engine} is not an available engine")

    @staticmethod
    def _set_def_options(engine, options):
        opts = EngineOptions._get_def_options(engine)
        if not EngineOptions._check(options):
            raise ValueError(f"Invalid option in {options}")
        defs = EngineOptions._get_def_options(engine)
        for opt in options:
            defs[opt] = options[opt]
        return defs

    @staticmethod
    def _set_options(engine, options):
        """Sets the option to be the given value.

        Arguments:
        option -- The dictionary of options and their values
        all_options -- The list of available options 
        """
        if not EngineOptions._check(options):
            raise ValueError(f"Invalid option in {options}")
        return options

    @staticmethod
    def _check(options):
        """Checks that the options are available for Skyhook

        Arguments:
        options -- A dictionary of options and their values
        all_options -- The list of available options
        """
        all_options = ('cls', 'quiet', 'start_obj',
                       'num_objs', 'output_format', 'program')
        for option in options:
            if option not in all_options:
                return False
        return True
