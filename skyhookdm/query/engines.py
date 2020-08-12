import os
import subprocess

from .models import SQLIR

class SkyhookRunQuery:
    """A class that builds Skyhook CLI commands and executes them."""

    @classmethod
    def create_sk_cmd(cls, query):
        """A function that yields a generator for the Skyhook CLI command from a Query Object. 

        Arguments:
        query   -- A Query dictionary  
        options -- A dictionary of Skyhook options

        Returns:
        skyhook_cmd -- A list of the arguments of the Skyhook run-query command stirng
        """
        if not isinstance(query, SQLIR):
            raise TypeError("Query must be of type SQLIR")

        command_args = [
            query.options['path_to_run_query'],
            
            '--num-objs'   , query.options['num-objs'],
            '--pool'       , query.options['pool'],
            '--oid-prefix' , "\"{}\"".format(query.options['oid-prefix']),
            '--table-name' , "\"{}\"".format(','.join(query.ir['table-name']))
        ]

        if query.options['header']:
            command_args.append("--header")

        if query.options['cls']:
            command_args.append("--use-cls")

        if query.options['quiet']:
            command_args.append("--quiet")

        if query.ir['projection']:
            projection = ','.join(query.ir['projection']).replace(' ', '')
            command_args.append("--project \"{}\" ".format(projection))

        if query.ir['selection']:
            predicates = ';'.join(query.ir['selection']).replace(' ', '')
            command_args.append("--select \"{}\"".format(predicates))

        return command_args

    @classmethod
    def execute_sk_cmd(cls, command_args):
        """A function that executes a Skyhook CLI command. 

        Arguments:
        command_args -- A list of arguments to be executed in which the first must be a path to Skyhook's run-query binary

        Returns: The stdout results of a subprocess execution of command_args 
        """
        cmd_completion = subprocess.run(command_args, check=True, stdout=subprocess.PIPE)
        return cmd_completion.stdout

    @classmethod
    def run_query(cls, query): 
        """A function that generates and executes a Skyhook CLI command from a query object. 

        Arguments: 
        query   -- A Query dictionary
        options -- A dictionary of Skyhook options 

        Returns: The results of a query execution
        """
        if not isinstance(query, SQLIR):
            raise TypeError("Query must be of type SQLIR")

        command_args = create_sk_cmd(query)

        return self.execute_command(command_args)
