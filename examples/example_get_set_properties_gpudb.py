#!/usr/bin/python

# #######################################
# Example snippet for /getsetproperties
# #######################################

from gpudb import GPUdb
import uuid


def example_get_set_properties():
    # Create a handle for GPUdb running on local machine
    gpudb = GPUdb( encoding='BINARY', gpudb_ip='127.0.0.1', gpudb_port='9191' )

    # Create a data type that can be used with GPUdb's built-in POINT semantic
    # (note the attributes called 'x' and 'y')
    custom_schema = """{
                            "type":"record",
                            "name":"custom_type",
                            "fields":[
                                {"name":"x","type":"double"},
                                {"name":"y","type":"double"}
                            ]
                        }""".replace(' ','').replace('\n','')

    # Register the above data type in GPUdb
    # Need to specify the semantic 'point'
    # Annotation and labels are irrelevant for this example
    register_response = gpudb.register_type( custom_schema, "", "", "POINT" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create a set ID (usually it's good to ensure that the set ID is unique)
    set_id = "get_set_properties_example" + str( uuid.uuid1() )

    # Create a new set using the above registered type (no parent ID needed)
    gpudb.new_set( type_id, set_id )

    # Fetch the properties of some sets
    response = gpudb.get_set_properties( [set_id, "MASTER"] )

    print
    print "Raw GPUdb get set properties response: \n", response
    print
    print "Formatted /getsetproperties response for two sets:"
    print
    print "set_ids:" # set IDs
    for set_id in response['set_ids']:
        print "\t", set_id
    print
    print "properties_maps:" # properties--the real return values
    for pmap in response['properties_maps']:
        for key, val in pmap.iteritems():
            print "\t", key, ": ", val
    print
    print "status_info:" # Default wrapper JSON stuff
    for key, val in response['status_info'].iteritems():
        print "\t", key, ": ", val

#   end function example_get_set_properties
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_get_set_properties()
