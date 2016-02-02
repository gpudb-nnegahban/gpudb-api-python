#!/usr/bin/python

# #####################################
# Example snippet for /random
# #####################################

from gaia import Gaia
import uuid


def example_random():
    # Create a handle for GPUdb running on local machine
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

    # Create a data type that can be used with GPUdb's built-in POINT semantic
    # (note the attributes called 'x' and 'y')
    custom_schema = """{
                            "type":"record",
                            "name":"custom_type",
                            "fields":[
                                {"name":"I1","type":"int"},
                                {"name":"I2","type":"int"},
                                {"name":"I3","type":"int"},
                                {"name":"I4","type":"int"},
                                {"name":"I5","type":"int"},
                                {"name":"L1","type":"long"},
                                {"name":"L2","type":"long"},
                                {"name":"L3","type":"long"},
                                {"name":"L4","type":"long"},
                                {"name":"L5","type":"long"},
                                {"name":"F1","type":"float"},
                                {"name":"F2","type":"float"},
                                {"name":"F3","type":"float"},
                                {"name":"F4","type":"float"},
                                {"name":"F5","type":"float"},
                                {"name":"D1","type":"double"},
                                {"name":"D2","type":"double"},
                                {"name":"D3","type":"double"},
                                {"name":"D4","type":"double"},
                                {"name":"D5","type":"double"},
                                {"name":"x","type":"double"},
                                {"name":"y","type":"double"},
                                {"name":"S1","type":"string"}
                            ]
                        }""".replace(' ','').replace('\n','')

    # Register the above data type in GPUdb
    # Need to specify the semantic 'point'
    # Annotation is irrelevant for this example
    # Label is only included so that the data can be viewed on Gaiademo
    register_response = gaiadb.register_type( custom_schema, "", "custom_point_type", "POINT" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create a set ID (usually it's good to ensure that the set ID is unique)
    set_id = "random_example" + str( uuid.uuid1() )

    # Create a new set using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = set_id )

    param_map = {"D1": {"min": 4},
                 "F5": {"max": -40},
                 "x": {"min": 7.7, "max": 25.25},
                 "y": {"min": 2.3, "linear_interval": 0.002},
                 "S1": {"min": 5, "max": 6} }
    # Generate 500 random objects
    random_response = gaiadb.random( set_id, 500, param_map )

    print
    print "Random function output: ", random_response

    # Note: User can view the data on a browser by using Gadmin

    # Verify that the set has 50 objects in it
    statistics_response = gaiadb.statistics(set_id = set_id, attribute = "x", stats = "count" )

    print
    print "Number of objects generated: ", statistics_response['stats']['count']


#   end function example_random
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_random( )
