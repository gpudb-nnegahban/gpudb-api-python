#!/usr/bin/python

################################################################
# Example snippet for:
#   /registertriggerrange
#   /clear_trigger
#   GPUdb.read_trigger_msg (python API only helper function)
#
# Also shows how to listen for trigger notifications
# by creating a simple zmq client.
################################################################

from gpudb import GPUdb
import uuid
import zmq
import threading, time


# Global variable to store the triggre notifications
received_trigger_notifications = []



def example_trigger_range():
    global received_trigger_notifications

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
    # Create a trigger ID (usually it's good to ensure that it is unique)
    trigger_id = "trigger_range_ID_" + str( uuid.uuid1() )

    # Create two new sets using the above registered type (no parent ID needed)
    gpudb.new_set( type_id, set_id1 )
    gpudb.new_set( type_id, set_id2 )

    # Create the client that would be listening to GPUdb for trigger notifications
    # ----------------------------------------------------------------------------
    # Find the trigger port for GPUdb
    ret_server_status = gpudb.server_status()
    gpudb_trigger_port = ret_server_status["status_map"]["conf.trigger_port"]
    gpudb_trigger_ip = "tcp://%s:%s" % ('127.0.0.1', gpudb_trigger_port)
    print "Trigger IP address: ", gpudb_trigger_ip
    print "Trigger port: '%s'" % gpudb_trigger_port

    # Create the client
    zclient = ClientTask( gpudb, gpudb_trigger_ip )
    zclient.start()

    # Register the trigger and then some points to the sets
    # -----------------------------------------------------
    # Create a trigger on 'x' in the range [0, 3]
    print "Creating a trigger with ID %s on sets:\n  %s\n  %s" \
        % (trigger_id, set_id1, set_id2)
    print "Trigger register output: ", gpudb.register_trigger_range( trigger_id, [set_id1, set_id2], "x", 0, 3 )

    # Add some objects to the two sets
    gpudb.bulk_add_point( set_id1,
                              [-1, 0, 0.2, 1.2, 2, 3, 3.4, -1.2, -0.2, 0.42],
                              [-1, 0, 0.2, 1.2, 2, 3, 3.4, -1.2, -0.2, 0.42] )
    gpudb.bulk_add_point( set_id2, range(-3, 10), range(-3, 10) )


    # Sleep for a little while
    time.sleep( 2 )

    # Print the received trigger notifciations
    print "Trigger notifications (%d objects):" % len (received_trigger_notifications)
    for n in received_trigger_notifications:
        print n

    # Clear the trigger
    print "Clearing the trigger; output:"
    print gpudb.clear_trigger( trigger_id )

    # Kill the client
    zclient.kill = True
    
#   end function example_trigger_range
#####################################################################



# Helper class for listening to trigger notifications from GPUdb
class ClientTask(threading.Thread):
    global received_trigger_notifications

    def __init__(self, gpudb, gpudb_trigger_ip):
        threading.Thread.__init__ (self)
        self.kill = False
        self.gpudb = gpudb
        self.gpudb_trigger_ip = gpudb_trigger_ip


    def run(self):
        # Create a client and listen for notifications
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        identity = 'worker-0'
        socket.setsockopt(zmq.IDENTITY, identity )
        socket.connect(self.gpudb_trigger_ip) # 'tcp://127.0.0.1:9001'
        socket.setsockopt(zmq.SUBSCRIBE, '')
        print 'Client %s started' % (identity)

        # Listen indefinitely until killed
        while True:
            if self.kill:
                break

            ret = socket.poll(250)
            if ret != 0:
                msg = socket.recv()
                # Read the trigger notification message
                msg_datum = self.gpudb.read_trigger_msg( msg )
                # Extract the data embedded within the message
                trigger_id = msg_datum['trigger_id']
                object_id  = msg_datum['object_id']
                received_trigger_notifications.append( [object_id, trigger_id] );

        print 'Client dying'
        socket.close()
        context.term()
# end class ClientTask ########################################


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_trigger_range()
