import os
import subprocess

class SkyhookRunner:
    """A class that builds Skyhook CLI commands and executes them."""

    @classmethod
    def create_sk_cmd(cls, query, options):
        """A function that yields a generator for the Skyhook CLI command from a Query Object. 

        Arguments:
        query   -- A Query dictionary  
        options -- A dictionary of Skyhook options

        Returns:
        skyhook_cmd -- A list of the arguments of the Skyhook run-query command stirng
        """
        command_args = [
            options['path_to_run_query'],
            
            '--num-objs'   , options['num-objs'],
            '--pool'       , options['pool'],
            '--oid-prefix' , "\"{}\"".format(options['oid-prefix']),
            '--table-name' , "\"{}\"".format(','.join(query['table-name']))
        ]

        if options['header']:
            command_args.append("--header")

        if options['cls']:
            command_args.append("--use-cls")

        if options['quiet']:
            command_args.append("--quiet")

        if query['projection']:
            projection = ','.join(query['projection'])
            command_args.append("--project \"{}\" ".format(projection))

        if query['selection']:
            predicates = ';'.join(query['selection'])
            command_args.append("--select \"{}\"".format(predicates))

        return command_args

    @classmethod
    def execute_sk_cmd(cls, command_args):
        """A function that executes a Skyhook CLI command. 

        Arguments:
        command_args -- A list of arguments to be executed in which the first must be a path to Skyhook's run-query binary

        Returns: The stdout results of a subprocess execution of command_args 
        """
        # result = os.popen("cd " + command).read()
        # return result
        cmd_completion = subprocess.run(command_args, check=True, stdout=subprocess.PIPE)
        return cmd_completion.stdout

    @classmethod
    def run_query(cls, query, options): 
        """A function that generates and executes a Skyhook CLI command from a query object. 

        Arguments: 
        query   -- A Query dictionary
        options -- A dictionary of Skyhook options 

        Returns: The results of a query execution
        """
        command_args = create_sk_cmd(query, options)

        return self.execute_command(command_args)

    @classmethod
    def package_arrow_objects(cls):
        """A function to coordinate the joining of arrow objects return from a Skyhook CLI command execution. 

        """
        raise NotImplementedError
