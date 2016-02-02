#!/usr/bin/python

# ########################################
# Example snippet for /updatesetmetadata
# ########################################

from gaia import Gaia
import uuid


def example_update_set_metadata():
    # Create a handle for GPUdb running on local machine
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

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
    register_response = gaiadb.register_type( custom_schema, "", "", "POINT" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create a set ID (usually it's good to ensure that the set ID is unique)
    set_id1 = "get_set_properties_example1_" + str( uuid.uuid1() )
    set_id2 = "get_set_properties_example2_" + str( uuid.uuid1() )
    set_id3 = "get_set_properties_example3_" + str( uuid.uuid1() )

    # Create a new set using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = set_id1 )
    gaiadb.new_set(type_id = type_id, set_id = set_id2 )
    gaiadb.new_set(type_id = type_id, set_id = set_id3 )

    # Update the metadata of the sets (two at once and another independently)
    response13 = gaiadb.update_set_metadata( [set_id1, set_id3], {"a": "A", "b": "B"} )
    response2 = gaiadb.update_set_metadata( [set_id2], {"two": "2"} )


    print
    print "----------------------------------------------------------"
    print "Raw GPUdb update set metadata response for sets 1 & 3: \n", response13
    print
    print "Formatted /updatesetmetadata response for two sets (1 & 3):"
    print
    print "set_ids:" # set IDs
    for set_id in response13['set_ids']:
        print "\t", set_id
    print
    print "metadata_map:" # metadata--the real return values
    for key, val in response13['metadata_map'].iteritems():
        print "\t", key, ": ", val
    print
    print "status_info:" # Default wrapper JSON stuff
    for key, val in response13['status_info'].iteritems():
        print "\t", key, ": ", val
    print "----------------------------------------------------------"
    print
    print "Raw GPUdb update set metadata response for set 2: \n", response2
    print
    print "Formatted /updatesetmetadata response for set 2:"
    print
    print "set_ids:" # set IDs
    for set_id in response2['set_ids']:
        print "\t", set_id
    print
    print "metadata_map:" # metadata--the real return values
    for key, val in response2['metadata_map'].iteritems():
        print "\t", key, ": ", val
    print
    print "status_info:" # Default wrapper JSON stuff
    for key, val in response2['status_info'].iteritems():
        print "\t", key, ": ", val
    print "----------------------------------------------------------"

#   end function example_update_set_metadata
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_update_set_metadata()
