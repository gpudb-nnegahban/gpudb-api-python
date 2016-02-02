#!/usr/bin/python

# #####################################
# Example snippet for /exit
# #####################################

from gaia import Gaia
import uuid


def example_exit():
    # Create a handle for GPUdb running on local machine
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

    # Exit GPUdb
    response = gaiadb.exit( "", "my_example_password" )

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
