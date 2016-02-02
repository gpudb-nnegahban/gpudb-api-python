#!/usr/bin/python

# #####################################
# Example snippet for /exit
# #####################################

from gpudb import GPUdb
import uuid


def example_exit():
    # Create a handle for GPUdb running on local machine
    gpudb = GPUdb( encoding='BINARY', gpudb_ip='127.0.0.1', gpudb_port='9191' )

    # Exit GPUdb
    response = gpudb.exit( "", "my_example_password" )

    print
    print "Raw GPUdb 'Exit' Response: ", response
    print
    print "Formatted /exit response:"
    print "exit_status: ", response['exit_status']

    print
    print "status_info:" # Default wrapper JSON stuff
    for key, val in response['status_info'].iteritems():
        print "\t", key, ": ", val


#   end function example_exit
#####################################################################



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':
    example_exit()
