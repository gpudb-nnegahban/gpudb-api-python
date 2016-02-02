#!/usr/bin/env python

# ######################################################
# 
# Python interface to GPUdb
#
# @file run_gpudb.py
# @author Meem Mahmud
# ######################################################

from gpudb import GPUdb
from gpudb import gpudb_obj_defs_path as gpudb_obj_defs_path # Path to obj_defs json files

import os
import sys
import argparse
import json

from avro import schema

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def run_gpudb( argv ):
    """An interface to GPUDB.  Run the specified query on GPUDB on the local
       machine or at the specified address.  Also provide usage information.
    """

    # Default values
    file_name = ""

    # Add arguments to the parser
    parser = argparse.ArgumentParser()
    parser.add_argument( '-g', nargs = '?', default = "127.0.0.1:9191",
                         help = "IP address and port of GPUdb in the format: xxx.xx.xx.xx:xxxx (defaults to 127.0.0.1:9191)" )
    parser.add_argument( '--request-path', nargs = '?', default = gpudb_obj_defs_path,
                         help = "Path of the JSON defintions (defaults to %s)" % gpudb_obj_defs_path )
    # User must provide one or the other
    query_group = parser.add_mutually_exclusive_group( required = True )
    query_group.add_argument( "--list-queries", action = 'store_true',
                         help = "Lists all available GPUDB queries." )
    query_group.add_argument( '--query', nargs = argparse.REMAINDER,
                         help = "Name of the query to be executed and any parameters associated with the query. For example, '--query max_min --attribute x --set_id set1'. Not providing any parameter after the query name will print query specific help information." )

    # Print the help message and quit if no arguments are given
    if ( len(sys.argv) == 1 ): # None provided
        parser.print_help()
        sys.exit( 2 )

    # Parse the command line arguments
    args = parser.parse_args()

    # Parse and check the request JSON path
    request_path = args.request_path
    if not os.path.exists( request_path ): # Check that the path exists
        print "Path for JSONs does not exist: ", request_path
        sys.exit( 2 )
    if request_path[-1] != "/": # simplify logic below by enforcing trailing '/'
        request_path += "/"

    # Create a list of all request JSON filenames
    filenames = [request_path + f for f in os.listdir( request_path ) if "_request.json" in f]

    # --------------------------------------
    # List all endpoint/query names, if desired by user
    if (args.list_queries == True) or (len(args.query) == 0):
        # Strip filename of the path and suffix if it's a request JSON file
        query_names = [ f.replace( request_path, "" ).replace( "_request.json", "" ) for f in filenames if "_request.json" in f ]
        for q in sorted( query_names ):
            print q
        sys.exit( 0 ) # Succesful termination after printing the desired help message
    # --------------------------------------


    # --------------------------------------
    # Set up GPUdb
    GPUdb_IP, GPUdb_Port = args.g.split( ":" )
    gpudbdb = GPUdb( encoding = 'BINARY', host = GPUdb_IP, port = GPUdb_Port )

    # Find and read the desired query JSON file
    query_name = args.query[ 0 ]
    for f in filenames:
        file_name = f if ("/" + query_name + "_request.json") in f else file_name
    if file_name == "":
        print "Query not found: ", query_name
        sys.exit( 2 )
    json_file = open( file_name, "r" )
    request_json = json_file.read()
    json_file.close()

    # Parse the request JSON to get the parameters
    request_schema = schema.parse( request_json )
    request_json =  request_schema.to_json()["fields"]

    # Create a dictionary of (param name, param type) pairs based on the JSON
    param_name_type = {}
    param_vals = {}
    # param_vals = collections.OrderedDict()
    for param in request_json:
        param_name_type[ param['name'] ] = param['type']
        # Binary/bytes parameters will be skipped
        if param['type'] == "string" or param['type'] == "bytes":
            param_vals[ param['name'] ] = "" # Default is empty string
        if param['type'] == "map":
            param_vals[ param['name'] ] = {} # Default is empty map
        if param['type'] == "list":
            param_vals[ param['name'] ] = [] # Default is empty list
        # Note that numeric attributes are not getting a default
        # User MUST provide such values, or we output an error

    # Create a parser for query-specific parameters
    query_parser = argparse.ArgumentParser()

    # Add parameters to be parsed
    query_parser.add_argument( "--format-response", action = 'store_true', dest = "format_response",
                             help = "Boolean parameter, include to print formatted GPUDB response. Omitting it prints the raw GPUDB response." )
    for pname, ptype in param_name_type.iteritems():
        if ptype == "string": # Make string arguments optional
            query_parser.add_argument( "--" + pname, nargs='?', default="", help = "Defaults to empty string" )
        elif ptype == "double" or ptype == "float":
            query_parser.add_argument( "--" + pname, type = float, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "long":
            query_parser.add_argument( "--" + pname, type = long, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "int":
            query_parser.add_argument( "--" + pname, type = int, required = True, help = "Required parameter, type %s" % ptype )
        elif ptype == "bytes":
            continue # ignore bytes
        elif ptype == "boolean": # Boolean flag
            # User must provide one or the other
            bool_group = query_parser.add_mutually_exclusive_group( required = True )
            bool_group.add_argument( "--" + pname, action = 'store_true', dest = pname,
                                       help = "Boolean parameter, include to set %s to TRUE" %pname )
            bool_group.add_argument( "--no-" + pname, action = 'store_false', dest = pname,
                                       help = "Boolean parameter, include to set %s to FALSE" % pname )
        else: # Maps and lists get empty ones by default; handling is delicate; ignore 'bytes'
            if ptype[ 'type' ] == "map":
                query_parser.add_argument( "--" + pname, nargs = '?', type = json.loads, default = {},
                                           help = "Expected map value of type: %s; surround the whole map with single quotes (') and any string (key or value) within with double quotes (\"). E.g. for random, --param_map '{\"x\":{\"min\":2}}'. When omitted, defaults to empty map" % ptype['values'] )
            else: # Arrays
                query_parser.add_argument( "--" + pname, type = json.loads, default=[],
                                           help = "Comma separated list (escape spaces with \) enclosed in []. For example, for filter_by_nai, --x_vector [1,2,3,4] or --x_vector [1,\ 2,\ 3,\ 4]. If contains strings, then enclose the whole thing within single quotes and the individual string in double quotes.  E.g., for filter_by_string, --attributes '[\"x\",\"y\"]'. When omitted, defaults to an empty list." )

    # Print the help message and quit if no arguments are given (and none is expected)
    if ( len( args.query[1:] ) == 0 and len( param_name_type ) > 0 ):
        print "No parameters provided for query: ", query_name
        query_parser.print_help()
        sys.exit( 2 )

    # Parse the parameters and store in a dictionary
    query_args = vars( query_parser.parse_args( args.query[1:] ) )

    # Copy the parsed values to the ordered dictionar to pass to GPUdb
    for key, val in query_args.iteritems():
        param_vals[ key ] = val
    # --------------------------------------


    # --------------------------------------
    # Call the GPUDB query:
    # Derive the endpoint name from the query name
    endpoint_name = "/" + query_name.replace( "_", "" )
    # One exception is /add
    if endpoint_name == "/addobject":
        endpoint_name = "/add"

    # Parse request and response schemas for GPUDB
    (req_schema, resp_schema) = gpudbdb.get_schemas( query_name )

    # Perform the GPUDB query
    response = gpudbdb.post_then_get( req_schema, resp_schema, param_vals, endpoint_name )

    print
    print "GPUDB Response:"
    if query_args[ "format_response" ] == True:
        print format_response( response )
    else:
        print response
    # --------------------------------------

# end run_gpudb



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def format_response( response, num_tabs = 0 ):
    """Format the gpudb response prettily for printing to screen
    """
    output = ""
    spaces = "    "

    # if isinstance( response, list ):

    for key, val in response.iteritems():
        # Embedded map
        if isinstance( val, dict ):
            output += num_tabs * spaces + key + ":\n"
            output += format_response( val, num_tabs + 1 )
        elif isinstance( val, list ):
            output += num_tabs * spaces + key + ":\n"
            num_tabs += 1
            for val_item in val: # iterate over the list
                if isinstance( val_item, dict ):
                    output += format_response( val_item, num_tabs )
                else: # 
                    output += num_tabs * spaces + val_item + "\n"
        else: # regular (key, val) pair => val is a scalar datatype
            output += num_tabs * spaces + "%s: %s\n" % (key, val)

    return output

# end format_response



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    run_gpudb( sys.argv )
