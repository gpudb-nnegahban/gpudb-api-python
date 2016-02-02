#!/usr/bin/python

################################################################
# Example snippet for:
#   /gettriggerinfo
# Also features:
#   /registertriggerrange
#   /clear_trigger
################################################################

from gpudb import GPUdb
import uuid





def example_get_trigger_info():

    # Create a handle for GPUdb running on local machine
    gpudb = GPUdb( encoding='BINARY', gpudb_ip='127.0.0.1', gpudb_port='9191' )

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
    register_response = gpudb.register_type( point_schema_str, "", "", "POINT" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create set IDs (usually it's good to ensure that the set ID is unique)
    set_id1 = "trigger_range_example_set_" + str( uuid.uuid1() )
    set_id2 = "trigger_range_example_set_" + str( uuid.uuid1() )
    # Create trigger IDs (usually it's good to ensure that it is unique)
    trigger_id1 = "trigger_range_ID_" + str( uuid.uuid1() )
    trigger_id2 = "trigger_range_ID_" + str( uuid.uuid1() )
    trigger_id3 = "trigger_range_ID_" + str( uuid.uuid1() )

    # Create two new sets using the above registered type (no parent ID needed)
    gpudb.new_set( type_id, set_id1 )
    gpudb.new_set( type_id, set_id2 )


    # Register the trigger and then some points to the sets
    # -----------------------------------------------------
    # Create a trigger on 'x' in the range [0, 3]
    gpudb.register_trigger_range( trigger_id1, [set_id1, set_id2], "x", 0, 3 )
    gpudb.register_trigger_range( trigger_id2, [set_id1], "x", -0.05, 30 )
    gpudb.register_trigger_range( trigger_id3, [set_id2], "x", -1, 2 )

    # Add some objects to the two sets
    gpudb.bulk_add_point( set_id1,
                              [-1, 0, 0.2, 1.2, 2, 3, 3.4, -1.2, -0.2, 0.42],
                              [-1, 0, 0.2, 1.2, 2, 3, 3.4, -1.2, -0.2, 0.42] )
    gpudb.bulk_add_point( set_id2, range(-5, 10), range(-5, 10) )

    # Retrieve trigger info
    print
    print "Info regarding trigger 1: ", gpudb.get_trigger_info( [trigger_id1] )

    print
    print "Formatted output for all trigger information:"
    trigger_info_map = gpudb.get_trigger_info( [] )["trigger_map"]
    for trigger, info_map in trigger_info_map.iteritems():
        print "Trigger ID: ", trigger
        for key, val in info_map.iteritems():
            print "    key: %s  val: " % key, val



    # Clear the trigger
    gpudb.clear_trigger( trigger_id1 )
    gpudb.clear_trigger( trigger_id2 )
    gpudb.clear_trigger( trigger_id3 )

#   end function example_get_trigger_info
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_get_trigger_info()
