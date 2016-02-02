#!/usr/bin/python

# #####################################
# Example snippet for /filterbyset
# #####################################

from gpudb import GPUdb
import uuid


def example_filter_by_set():
    # Create a handle for GPUdb running on local machine
    gpudb = GPUdb( encoding='BINARY', gpudb_ip='127.0.0.1', gpudb_port='9191' )

    # Create two data types
    first_schema = """{
                            "type":"record",
                            "name":"custom_type",
                            "fields":[
                                {"name":"first_name","type":"string"}
                            ]
                        }""".replace(' ','').replace('\n','')
    last_schema = """{
                            "type":"record",
                            "name":"custom_type",
                            "fields":[
                                {"name":"last_name","type":"string"}
                            ]
                        }""".replace(' ','').replace('\n','')

    # Register the above data types in GPUdb
    # Annotation and labels are irrelevant for this example
    register_response1 = gpudb.register_type( first_schema, "", "", "GENERICOBJECT" )
    register_response2 = gpudb.register_type( last_schema, "", "", "GENERICOBJECT" )

    # Extract the type IDs from the above responses
    type_id1 = register_response1[ 'type_id' ]
    type_id2 = register_response2[ 'type_id' ]

    # Create a set ID (usually it's good to ensure that the set ID is unique)
    first_set_id = "filter_by_set_example_first_" + str( uuid.uuid1() )
    last_set_id = "filter_by_set_example_last_" + str( uuid.uuid1() )

    # Create new sets using the above registered types (no parent ID needed)
    gpudb.new_set( type_id1, first_set_id )
    gpudb.new_set( type_id2, last_set_id )

    # Generate 100 random objects (limit the string lengths)
    gpudb.random( first_set_id, 40000, {"first_name": {"min": 4, "max": 12}} )
    gpudb.random( last_set_id, 40000, {"last_name": {"min": 4, "max": 12}} )

    # Note: User can easily view the data on a browser by using Gadmin

    # Perform filter by set on last names using first names: find last names that also
    # can be first names (e.g. Stewart)
    result_set_id = "filtered_last_names_" + str( uuid.uuid1())
    response = gpudb.filter_by_set( last_set_id, result_set_id, "last_name",
                                        first_set_id, "first_name" )

    print response
    
    print
    print "Number of last names that sound like first names: ", response['count']


#   end function example_filter_by_set
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_filter_by_set()
