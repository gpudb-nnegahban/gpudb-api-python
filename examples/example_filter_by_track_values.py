#!/usr/bin/python

# #########################################
# Example snippet for /filterbytrackvalues
# #########################################

from gaia import Gaia
import uuid
import json


def example_filter_by_track_values():
    # Create a handle for GPUdb running on local machine
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

    # Create a track data type
    track_schema = """{
                         "type":"record",
                         "name":"my_track",
                         "fields":[
                                {"name":"TRACKID","type":"string"},
                                {"name":"x","type":"double"},
                                {"name":"y","type":"double"},
                                {"name":"TIMESTAMP","type":"long"}
                            ]
                        }""".replace(' ','').replace('\n','')

    # Register the above data type in GPUdb
    # Annotation and labels are irrelevant for this example
    register_response = gaiadb.register_type( track_schema, "", "", "TRACK" )

    # Extract the type ID from the above response
    type_id = register_response[ 'type_id' ]

    # Create a set ID (usually it's good to ensure that the set ID is unique)
    track_set_id = "filter_by_track_values_example_" + str( uuid.uuid1() )

    # Create new sets using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = track_set_id )

    # Generate 100 random track objects; bound the points within a small box
    # so that the filter catches a few points later
    gaiadb.random( track_set_id, 100, {"x": {"min": 51, "max": 52},
                                          "y": {"min": -10, "max": -9},
                                          "TIMESTAMP": {"min": 2346577918000, "max": 2346577918100}} )

    # Note: User can easily view the data on a browser by using Gadmin

    # Name of the result set to be created (make unique using a UUID)
    result_set_id = "filtered_by_track_values_" + str( uuid.uuid1())

    # Spatial and temporal search parameters, including the spatial metric
    param_map = {"spatial_radius": "50000", "time_radius": "9000", "spatial_distance_metric": "great_circle"}

    # Values to search for
    x_vals = [51.1, 51.2, 51.3, 51.4, 51.5, 51.6, 51.7, 51.8, 51.9 ]
    y_vals = [-9.1, -9.2, -9.3, -9.4, -9.5, -9.6, -9.7, -9.8, -9.9 ]
    t_vals = [2346577918010, 2346577918020, 2346577918030, 2346577918040, 2346577918050, 2346577918060, 2346577918070, 2346577918080, 2346577918090]

    # Perform filter by track (not providing a "target" track id; want to match any track with the given one)
    response = gaiadb.filter_by_track_values( track_set_id,
                                                 x_vals, y_vals, t_vals,
                                                 [], [], param_map, result_set_id )

    print
    print "The raw response: ", response
    
    print
    print "Number of points that pass through the filter: ", response['count']

#   end function example_filter_by_track_values
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_filter_by_track_values()
