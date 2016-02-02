#!/usr/bin/python

# #####################################
# Example snippet for /statistics
# #####################################

from gaia import Gaia
import uuid


def example_statistics():
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
    set_id = "statistics_example" + str( uuid.uuid1() )

    # Create a new set using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = set_id )

    # Generate 100 random objects (parameter param_map is empty)
    gaiadb.random( set_id, 1000, {"x": {"min": 10, "max": 10}} )

    # Note: User can view the data on a browser by using Gadmin

    # Run some statistics on it
    # 'count' will be returned by default (i.e. even when not explicitly asked for)
    statistics_response_x = gaiadb.statistics(set_id = set_id, attribute = "x", stats = "sum,mean,stdv,variance,skew,kurtosis,cardinality,estimated_cardinality" )
    # The user can explicitly ask for 'count' as well
    statistics_response_y = gaiadb.statistics(set_id = set_id, attribute = "y", stats = "count,sum,mean,stdv,variance,skew,kurtosis,cardinality,estimated_cardinality" )

    print
    print "Raw GPUdb Statistics Response for x: ", statistics_response_x
    print
    print "Formatted /statistics response for x (note the difference between count and cardinality based on the restriction on the distribution of x values):"
    for stat_name, stat_val in statistics_response_x['stats'].iteritems():
        print stat_name, ": ", stat_val

    print
    print "Raw GPUdb Statistics Response for y: ", statistics_response_y
    print
    print "Formatted /statistics response for y:"
    for stat_name, stat_val in statistics_response_y['stats'].iteritems():
        print stat_name, ": ", stat_val

#   end function example_statistics
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_statistics( )
