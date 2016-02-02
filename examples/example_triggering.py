#!/usr/bin/python

# #################################
# Example snippet for:
#   /registertriggerrange
#   /registertriggernai
#   /gettriggerinfo
#   /clear_trigger
#   Gaia.read_trigger_msg
# #################################

from gaia import Gaia
import uuid


def example_trigger_range_clear():
    # Create a handle for GPUdb running on local machine
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

    # Create a data type that can be used with GPUdb's built-in POINT semantic
    point_schema_str = """{
                              "type":"record",
                              "name":"point",
                              "fields":
                               [
                                 {"name":"x","type":"double"},
                                 {"name":"y","type":"double"},
                                 {"name":"OBJECT_ID","type":"string"}
                               ]
                            }""".replace(' ','').replace('\n','')

    # Register the above data type in GPUdb
    # Need to specify the semantic 'point'
    # Annotation and labels are irrelevant for this example
    register_response = gaiadb.register_type( point_schema_str, "", "", "POINT" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create set IDs (usually it's good to ensure that the set ID is unique)
    set_id1 = "trigger_range_example_set_" + str( uuid.uuid1() )
    set_id2 = "trigger_range_example_set_" + str( uuid.uuid1() )
    # Create a trigger ID (usually it's good to ensure that it is unique)
    trigger_id = "trigger_range_ID_" + str( uuid.uuid1() )

    # Create two new sets using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = set_id1 )
    gaiadb.new_set(type_id = type_id, set_id = set_id2 )

    # Create a trigger on 'x' in the range [0, 3]
    print "Creating a trigger with ID %s; output:" % trigger_id
    print gaiadb.register_trigger_range( trigger_id, [set_id1, set_id2], "x", 0, 3 )

    # Generate random objects in the two sets
    param_map = {"x": {"min_val": -2, "max_val": 7}}
    gaiadb.random( set_id1, 50, param_map )
    param_map = {"x": {"min_val": 1, "max_val": 10}}
    gaiadb.random( set_id1, 100, param_map )

    


    # Note: User can view the data on a browser by using Gadmin

    # Verify that the set has 50 objects in it
    statistics_response = gaiadb.statistics(set_id = set_id, attribute = "x", stats = "count" )

    print "Set cardinality: ", statistics_response['stats']['count']

#   end function example_trigger_range_clear
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_trigger_range_clear()
