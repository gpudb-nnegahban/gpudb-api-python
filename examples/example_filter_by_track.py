#!/usr/bin/python

# #####################################
# Example snippet for /filterbytrack
# #####################################

from gaia import Gaia
import uuid
import json


def example_filter_by_track():
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
    track_set_id = "filter_by_track_example_" + str( uuid.uuid1() )

    # Create new sets using the above registered type (no parent ID needed)
    gaiadb.new_set(type_id = type_id, set_id = track_set_id )

    # Generate 100 random track objects; bound the points within a small box
    # so that the filter catches a few points later
    gaiadb.random( track_set_id, 100, {"x": {"min": 50, "max": 60},
                                          "y": {"min": -9, "max": 13},
                                          "TIMESTAMP": {"min": 4788144933296300000, "max": 4788144933296355706}} )

    # Note: User can easily view the data on a browser by using Gadmin

    # Obtain a track ID for a random track:
    # Get an object from the set
    get_set_obj_response = gaiadb.get_set_objects( track_set_id, 0, 1, encoding = "json" )
    # Extract the string that represents the first track object
    track_str = get_set_obj_response['list_str'][0]
    # Convert it into a JSON
    track_json = json.loads( track_str )
    # Extract the track ID
    track_id = track_json["TRACKID"]

    result_set_id = "filtered_by_track_" + str( uuid.uuid1())

    param_map = {"spatial_radius": "50000", "time_radius": "9000", "spatial_distance_metric": "great_circle"}


    # Perform filter by track (not providing a "target" track id; want to match any track with the given one)
    response = gaiadb.filter_by_track( track_set_id, track_id, [], param_map, result_set_id )

    print
    print "The raw response: ", response
    
    print
    print "Number of points that pass through the filter: ", response['count']


#   end function example_filter_by_track
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_filter_by_track()
