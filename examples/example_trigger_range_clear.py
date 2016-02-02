#!/usr/bin/python

# ############################################################
# Example snippet for:
#   /registertriggerrange
#   /clear_trigger
#   GPUdb.read_trigger_msg (helper python only function)
#
# Also shows how to listen for trigger notifications
# by creating a simple zmq client.
# ############################################################

from gaia import Gaia
import uuid
import zmq
import threading, time


# Global variable to store the triggre notifications
received_trigger_notifications = []



def example_trigger_range():
    global received_trigger_notifications

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

    # Create the client that would be listening to GPUdb for trigger notifications
    # ----------------------------------------------------------------------------
    # Find the trigger port for GPUdb
    ret_server_status = gaiadb.server_status()
    gaiadb_trigger_port = ret_server_status["status_map"]["conf.trigger_port"]
    gaiadb_trigger_ip = "tcp://%s:%s" % ('127.0.0.1', gaiadb_trigger_port)
    print "Trigger IP address: ", gaiadb_trigger_ip
    print "Trigger port: '%s'" % gaiadb_trigger_port

    # Create the client
    zclient = ClientTask( gaiadb, gaiadb_trigger_ip )
    zclient.start()
    # --------------------------------------------------------

    # Sleep for a little while
    time.sleep( 1 )

    # Register the trigger and then some points to the sets
    # -----------------------------------------------------
    # Create a trigger on 'x' in the range [0, 3]
    print "Creating a trigger with ID %s on sets:\n  %s\n  %s" \
        % (trigger_id, set_id1, set_id2)
    print "Trigger register output: ", gaiadb.register_trigger_range( trigger_id, [set_id1, set_id2], "x", 0, 3 )

    # Sleep for a little while
    time.sleep( 1 )


    # Generate random objects in the two sets (should trip the trigger)
    param_map = {"x": {"min_val": -2, "max_val": 7}}
    gaiadb.random( set_id1, 100, param_map )
    param_map = {"x": {"min_val": 1, "linear_interval": 0.1}}
    gaiadb.random( set_id2, 100, param_map )

    # Sleep for a little while
    time.sleep( 10 )

    # Print the received trigger notifciations
    print "Trigger notifications:"
    for n in received_trigger_notifications:
        print n

    # Clear the trigger
    print "Clearing the trigger; output:"
    print gaiadb.clear_trigger( trigger_id )

    # Kill the client
    zclient.kill = True
    
#   end function example_trigger_range
#####################################################################



# Helper class for listening to trigger notifications from GPUdb
class ClientTask(threading.Thread):
    global received_trigger_notifications

    def __init__(self, gaiadb, gaiadb_trigger_ip):
        threading.Thread.__init__ (self)
        self.kill = False
        self.gaiadb = gaiadb
        # self.gaiadb = Gaia( encoding = gaiadb.encoding, gaia_ip = gaiadb.gaia_ip,
        #                     gaia_port = gaiadb.gaia_port )
        self.gaiadb_trigger_ip = gaiadb_trigger_ip


    def run(self):
        # Create a client and listen for notifications
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        identity = 'worker-0'
        socket.setsockopt(zmq.IDENTITY, identity )
        socket.connect(self.gaiadb_trigger_ip) # 'tcp://127.0.0.1:9001'
        socket.setsockopt(zmq.SUBSCRIBE, '')
        print 'Client %s started' % (identity)

        # Listen indefinitely until killed
        while True:
            print "client running"
            if self.kill:
                break

            ret = socket.poll(250)
            print "client socket poll result: ", ret
            if ret != 0:
                msg = socket.recv()
                # Read the trigger notification message
                msg_datum = self.gaiadb.read_trigger_msg( msg )
                # Extract the data embedded within the message
                trigger_id = msg_datum['trigger_id']
                object_id  = msg_datum['object_id']
                received_trigger_notifications.append( [object_id, trigger_id] );


        print "# of pairs in the notifications list: ", len (received_trigger_notifications)

        print 'Client dying'
        socket.close()
        context.term()
# end class ClientTask ########################################


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_trigger_range()
