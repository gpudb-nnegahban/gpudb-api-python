# ---------------------------------------------------------------------------
# gpudb.py - The Python API to interact with a GPUdb server. 
#
# Copyright (c) 2014 GIS Federal
# ---------------------------------------------------------------------------

import cStringIO, StringIO
import base64, httplib
import os, sys
import json
import uuid

# ---------------------------------------------------------------------------
# The absolute path of this gpudb.py module for importing local packages
gpudb_module_path = __file__
if gpudb_module_path[len(gpudb_module_path)-3:] == "pyc": # allow symlinks to gpudb.py
    gpudb_module_path = gpudb_module_path[0:len(gpudb_module_path)-1]
if os.path.islink(gpudb_module_path): # allow symlinks to gpudb.py
    gpudb_module_path = os.readlink(gpudb_module_path)
gpudb_module_path = os.path.dirname(os.path.abspath(gpudb_module_path))

# Search for our modules first, probably don't need imp or virt envs.
if not gpudb_module_path + "/packages" in sys.path :
    sys.path.insert(1, gpudb_module_path + "/packages")

# ---------------------------------------------------------------------------
# Local imports after adding our module search path
from avro import schema, datafile, io


if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections # a separate package

have_snappy = False
try:
    import snappy
    have_snappy = True
except ImportError:
    have_snappy = False

from tabulate import tabulate

# ---------------------------------------------------------------------------
# GPUdb - Lightweight client class to interact with a GPUdb server.
# ---------------------------------------------------------------------------

class GPUdb:

    def __init__(self, host="127.0.0.1", port="9191",
                       encoding="BINARY", connection='HTTP',
                       username="", password=""):
        """
        Construct a new GPUdb client instance.

        Parameters:
            host    : The IP address of the GPUdb server.
            port  : The port of the GPUdb server at the given IP address.
            encoding   : Type of Avro encoding to use, "BINARY", "JSON" or "SNAPPY".
            connection : Connection type, currently only "HTTP" or "HTTPS" supported.
            username   : An optional http username.
            password   : The http password for the username.
        """

        assert (type(host) is str), "Expected a string host address, got: '"+str(host)+"'"

        # host may take the form of :
        #  - "https://user:password@domain.com:port/path/"

        if host.startswith("http://") :    # Allow http://, but remove it.
            host = host[7:]
        elif host.startswith("https://") : # Allow https://, but remove it.
            host = host[8:]
            connection = "HTTPS" # force it

        # Parse the username and password, if supplied.
        host_at_sign_pos = host.find('@')
        if host_at_sign_pos != -1 :
            user_pass = host[:host_at_sign_pos]
            host = host[host_at_sign_pos+1:]
            user_pass_list = user_pass.split(':')
            username = user_pass_list[0]
            if len(user_pass_list) > 1 :
                password = user_pass_list[1]

        url_path = ""
        # Find the URL /path/ and remove it to get the ip address.
        host_path_pos = host.find('/')
        if host_path_pos != -1:
            url_path = host[host_path_pos:]
            if url_path[-1] == '/':
                url_path = url_path[:-1]
            host = host[:host_path_pos]

        # Override default port if specified in ip address
        host_port_pos = host.find(':')
        if host_port_pos != -1 :
            port = host[host_port_pos+1:]
            host = host[:host_port_pos]

        # Port does not have to be provided if using standard HTTP(S) ports.
        if (port == None) or len(str(port)) == 0:
            if connection == 'HTTP' :
                port = 80
            elif connection == 'HTTPS' :
                port = 443

        # Validate port
        try :
            port = int(port)
        except :
            assert False, "Expected a numeric port, got: '" + str(port) + "'"

        assert (port > 0) and (port < 65536), "Expected a valid port (1-65535), got: '"+str(port)+"'"
        assert (len(host) > 0), "Expected a valid host address, got an empty string."
        assert (encoding in ["BINARY", "JSON", "SNAPPY"]), "Expected encoding to be either 'BINARY', 'JSON' or 'SNAPPY' got: '"+str(encoding)+"'"
        assert (connection in ["HTTP", "HTTPS"]), "Expected connection to be 'HTTP' or 'HTTPS', got: '"+str(connection)+"'"

        if (encoding == 'SNAPPY' and not have_snappy):
            print 'SNAPPY encoding specified but python-snappy is not installed; reverting to BINARY'
            encoding = 'BINARY'

        self.host       = host
        self.port       = int(port)
        self.encoding   = encoding
        self.connection = connection
        self.username   = username
        self.password   = password
        self.gpudb_url_path = url_path


        self.client_to_object_encoding_map = { \
                                               "BINARY": "binary",
                                               "SNAPPY": "binary",
                                               "JSON": "json",
        }

        # Load all gpudb schemas
        self.load_gpudb_schemas()
    # end __init__


    # members
    host       = "127.0.0.1" # Input host with port appended if provided.
    gpudb_url_path = ""          # Input /path (if any) that was in the host.
    port     = "9191"      # Input port, may be empty.
    encoding      = "BINARY"    # Input encoding, either 'BINARY' or 'JSON'.
    connection    = "HTTP"      # Input connection type, either 'HTTP' or 'HTTPS'.
    username      = ""          # Input username or empty string for none.
    password      = ""          # Input password or empty string for none.

    # constants
    END_OF_SET = -9999

    # schemas for common data types
    point_schema_str = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"OBJECT_ID","type":"string"}]}"""
    big_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    gis_point_schema_str = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    bytes_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"},{"name":"OBJECT_ID","type":"string"}]}"""
    bigger_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    twitter_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"KEYWORD","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""

    # Some other schemas for internal work
    logger_request_schema_str = """
        {
            "type" : "record", 
            "name" : "logger_request",
            "fields" : [
                {"name" : "ranks", "type" : {"type" : "array", "items" : "int"}},
                {"name" : "log_levels", "type" : {"type" : "map", "values" : "string"}}
            ]
        }
    """.replace("\n", "").replace(" ", "")
    logger_response_schema_str = """
        {
            "type" : "record", 
            "name" : "logger_response",
            "fields" : [
                {"name" : "status" , "type" : "string"},
                {"name" : "log_levels", "type" : {"type" : "map", "values" : "string"}}
            ]
        }
    """.replace("\n", "").replace(" ", "")

    # Parse common schemas, others parsed on demand.
    point_schema = schema.parse(point_schema_str)
    big_point_schema = schema.parse(big_point_schema_str)
    gis_point_schema = None # schema.parse(gis_point_schema_str)
    bytes_point_schema = None # schema.parse(bytes_point_schema_str)
    bigger_point_schema = None # schema.parse(bigger_point_schema_str)
    twitter_point_schema = schema.parse(twitter_point_schema_str)

    # -----------------------------------------------------------------------
    # Helper functions
    # -----------------------------------------------------------------------

    def post_to_gpudb_read(self, body_data, endpoint):
        """
        Create a HTTP connection and POST then get GET, returning the server response.

        Parameters:
            body_data : Data to POST to GPUdb server.
            endpoint  : Server path to POST to, e.g. "/add".
        """

        if self.encoding == 'BINARY':
            headers = {"Content-type": "application/octet-stream",
                       "Accept": "application/octet-stream"}
        elif self.encoding == 'JSON':
            headers = {"Content-type": "application/json",
                       "Accept": "application/json"}
        elif self.encoding == 'SNAPPY':
            headers = {"Content-type": "application/x-snappy",
                       "Accept": "application/x-snappy"}
            body_data = snappy.compress(body_data)

        if len(self.username) != 0:
            # base64 encode the username and password
            auth = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
            headers["Authorization"] = ("Basic %s" % auth)

        # NOTE: Creating a new httplib.HTTPConnection is suprisingly just as
        #       fast as reusing a persistent one and has the advantage of
        #       fully retrying from scratch if the connection fails.

        try:
            if (self.connection == 'HTTP'):
                conn = httplib.HTTPConnection(host=self.host, port=self.port)
            elif (self.connection == 'HTTPS'):
                conn = httplib.HTTPSConnection(host=self.host, port=self.port)
            else:
                assert False, "Unknown connection type, should be 'HTTP' or 'HTTPS'"
        except:
            print("Error connecting to: '%s' on port %d" % (self.host, self.port))
            raise

        try:
            conn.request("POST", self.gpudb_url_path+endpoint, body_data, headers)
        except:
            print("Error posting to: '%s:%d%s'" % (self.host, self.port, self.gpudb_url_path+endpoint))
            raise

        try:
            resp = conn.getresponse()
            resp_data = resp.read()
            #print 'data received: ',len(resp_data)
            #print 'headers received: ',resp.getheaders()
            resp_time = resp.getheader('x-request-time-secs',None)
        except: # some error occurred; return a message
            # TODO: Maybe use a class like GPUdbException
            raise ValueError( "Timeout Error: No response received from %s" % self.host )
        # end except

        # resp = conn.getresponse()
        # #Print resp.status,resp.reason
        # resp_data = resp.read() # TODO: comment this out
        # #print("response size: %d"   % (len(resp_data)))
        # #print("response     : '%s'" % (resp_data))

        return  str(resp_data),resp_time

    def write_datum(self, SCHEMA, datum):
        """
        Returns an avro binary or JSON encoded dataum dict using its schema.

        Parameters:
            SCHEMA : A parsed schema from avro.schema.parse().
            datum  : A dict of key-value pairs matching the schema.
        """

        # build the encoder; this output is where the data will be written
        if self.encoding == 'BINARY' or self.encoding == 'SNAPPY':
            output = cStringIO.StringIO()
            be = io.BinaryEncoder(output)

            # Create a 'record' (datum) writer
            writer = io.DatumWriter(SCHEMA)
            writer.write(datum, be)

            return output.getvalue()

        elif self.encoding == 'JSON':

            data_str = json.dumps(datum)

            return data_str

    def encode_datum(self, schema_str, datum):
        OBJ_SCHEMA = schema.parse(schema_str)

        return self.write_datum(OBJ_SCHEMA, datum)


    def client_to_object_encoding( self ):
        """Returns object encoding for queries based on the GPUdb client's
        encoding.
        """
        return self.client_to_object_encoding_map[ self.encoding ]
    # end client_to_object_encoding

    def read_orig_datum(self, SCHEMA, encoded_datum, encoding=None):
        """
        Decode the binary or JSON encoded datum using the avro schema and return a dict.

        Parameters:
            SCHEMA        : A parsed schema from avro.schema.parse().
            encoded_datum : Binary or JSON encoded data.
            encoding      : Type of avro encoding, either "BINARY" or "JSON",
                            None uses the encoding this class was initialized with.
        """
        if encoding == None:
            encoding = self.encoding

        if (encoding == 'BINARY') or (encoding == 'SNAPPY'):
            output = cStringIO.StringIO(encoded_datum)
            bd = io.BinaryDecoder(output)
            reader = io.DatumReader(SCHEMA)
            out = reader.read(bd) # read, give a decoder

            return out
        elif encoding == 'JSON':
            data_str = json.loads(encoded_datum.replace('\\U','\\u'))

            return data_str


    def read_datum(self, SCHEMA, encoded_datum, encoding=None, response_time=None):
        """
        Decode a gpudb_response and decode the contained message too.

        Parameters:
            SCHEMA : The parsed schema from avro.schema.parse() that the gpudb_response contains.
            encoded_datum : A BINARY or JSON encoded gpudb_response message.
        Returns:
            An OrderedDict of the decoded gpudb_response message's data with the
            gpudb_response put into the "status_info" field.
        """

        # Parse the gpudb_response message
        REP_SCHEMA = self.gpudb_schemas["gpudb_response"]["RSP_SCHEMA"]
        resp = self.read_orig_datum(REP_SCHEMA, encoded_datum, encoding)

        #now parse the actual response if there is no error
        #NOTE: DATA_SCHEMA should be equivalent to SCHEMA but is NOT for get_set_sorted
        stype = resp['data_type']
        if stype == 'none':
            out = collections.OrderedDict()
        else:
            if self.encoding == 'JSON':
                out = self.read_orig_datum(SCHEMA, resp['data_str'], 'JSON')
            elif (self.encoding == 'BINARY') or (self.encoding == 'SNAPPY'):
                out = self.read_orig_datum(SCHEMA, resp['data'], 'BINARY')

        del resp['data']
        del resp['data_str']

        out['status_info'] = resp

        if (response_time is not None):
            out['status_info']['response_time'] = float(response_time)

        return out

    def get_schemas(self, base_name):
        """
        Get a tuple of parsed and cached request and reply schemas.

        Parameters:
            base_name : Schema name, e.g. "base_name"+"_request.json" or "_response.json"
        """
        REQ_SCHEMA = self.gpudb_schemas[base_name]["REQ_SCHEMA"]
        RSP_SCHEMA = self.gpudb_schemas[base_name]["RSP_SCHEMA"]
        return (REQ_SCHEMA, RSP_SCHEMA)


    def post_then_get(self, REQ_SCHEMA, REP_SCHEMA, datum, endpoint):
        """
        Encode the datum dict using the REQ_SCHEMA, POST to GPUdb server and
        decode the reply using the REP_SCHEMA.

        Parameters:
            REQ_SCHEMA : The parsed schema from avro.schema.parse() of the request.
            REP_SCHEMA : The parsed schema from avro.schema.parse() of the reply.
            datum      : Request dict matching the REQ_SCHEMA.
            endpoint   : Server path to POST to, e.g. "/add".
        """
        encoded_datum = self.write_datum(REQ_SCHEMA, datum)
        response,response_time  = self.post_to_gpudb_read(encoded_datum, endpoint)

        return self.read_datum(REP_SCHEMA, response, None, response_time)

    # ------------- Convenience Functions ------------------------------------

    def read_point(self, encoded_datum, encoding=None):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        return self.read_orig_datum(self.point_schema, encoded_datum, encoding)

    def read_big_point(self, encoded_datum, encoding=None):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        return self.read_orig_datum(self.big_point_schema, encoded_datum, encoding)

    def read_gis_point(self, encoded_datum, encoding=None):
        # this point is designed to look like "Point"

        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        return self.read_orig_datum(self.gis_point_schema, encoded_datum, encoding)

    def read_trigger_msg(self, encoded_datum):
        RSP_SCHEMA = self.gpudb_schemas[ "trigger_notification" ]["RSP_SCHEMA"]
        return self.read_orig_datum(RSP_SCHEMA, encoded_datum, 'BINARY')


    def logger(self, ranks, log_levels):
        """Convenience function to change log levels of some
        or all GPUdb ranks.
        """
        REQ_SCHEMA     = schema.parse( self.logger_request_schema_str )
        REP_SCHEMA     = schema.parse( self.logger_response_schema_str )

        datum = collections.OrderedDict()
        datum["ranks"]      = ranks
        datum["log_levels"] = log_levels

        print('Using host: %s\n' % (self.host))
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/logger")
    # end logger

    # ------------------ Type registration functions for convenient types ------

    def register_type_big_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.big_point_schema_str
        #datum["annotation"] = "msg_id"
        datum["label"] = "big_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    def register_type_bigger_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.bigger_point_schema_str
        #datum["annotation"] = "ARTIFACTID"
        datum["label"] = "bigger_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    def register_type_bytes_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.bytes_point_schema_str
        #datum["annotation"] = "msg_id"
        datum["label"] = "bytes_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    def register_type_gis_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.gis_point_schema_str
        #datum["annotation"] = "msg_id"
        datum["label"] = "gis_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    def register_type_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.point_schema_str
        #datum["annotation"] = ""
        datum["label"] = "basic_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    def register_type_twitter_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("create_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.twitter_point_schema_str
        #datum["annotation"] = "ARTIFACTID"
        datum["label"] = "twitter_point_type"
        datum["properties"] = {}
        datum["options"] = {}

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/create/type")

    # ------------------ Add functions for convenient types -----------------------------

    def add_big_point(self, set_id, msg_id, x, y, timestamp, source, group_id, OBJECT_ID=''):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        obj_list_encoded = []

        datum = collections.OrderedDict()
        datum["msg_id"] = msg_id
        datum["x"] = x
        datum["y"] = y
        datum["TIMESTAMP"] = timestamp
        datum["source"] = source
        datum["group_id"] = group_id
        datum["OBJECT_ID"] = OBJECT_ID

        obj_list_encoded.append(self.write_datum(self.big_point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})

    def add_bigger_point(self, set_id, artifact_id, x, y, timestamp, OBJECT_ID=''):
        if self.bigger_point_schema is None:
            self.bigger_point_schema = schema.parse(self.bigger_point_schema_str)

        obj_list_encoded = []

        datum = collections.OrderedDict()
        datum["ARTIFACTID"] = artifact_id
        datum["x"] = x
        datum["y"] = y
        datum["TIMESTAMP"] = timestamp
        datum["DATASOURCE"] = "OSC"
        datum["DATASOURCESUB"] = "REPLICATED"
        datum["DATASOURCEKEY"] = "OSC:REPLICATED"
        datum["AUTHOR"] = "OSC"
        datum["OBJECTAUTH"] = "U"
        datum["OBJECT_ID"] = OBJECT_ID

        obj_list_encoded.append(self.write_datum(self.bigger_point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})


    def add_bytes_point(self, set_id, msg_id, x, y, timestamp, source, group_id, bytes_data, OBJECT_ID=''):
        if self.bytes_point_schema is None:
            self.bytes_point_schema = schema.parse(self.bytes_point_schema_str)

        obj_list_encoded = []

        datum = collections.OrderedDict()
        datum["msg_id"] = msg_id
        datum["x"] = x
        datum["y"] = y
        datum["timestamp"] = timestamp
        datum["source"] = source
        datum["group_id"] = group_id
        datum["bytes_data"] = bytes_data
        datum["OBJECT_ID"] = OBJECT_ID

        obj_list_encoded.append(self.write_datum(self.bytes_point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})


    def add_gis_point(self, set_id, msg_id, x, y, timestamp, tag_id, derived, group_id,
                         level_one_mgrs, level_two_mgrs, level_three_mgrs, level_final_mgrs, OBJECT_ID=''):
        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        obj_list_encoded = []

        datum = collections.OrderedDict()

        datum["x"] = x
        datum["y"] = y
        datum["timestamp"] = timestamp
        datum["tag_id"] = tag_id
        datum["derived"] = derived
        datum["msg_id"] = msg_id
        datum["group_id"] = group_id
        datum["level_one_mgrs"] = level_one_mgrs
        datum["level_two_mgrs"] = level_two_mgrs
        datum["level_three_mgrs"] = level_three_mgrs
        datum["level_final_mgrs"] = level_final_mgrs
        datum["OBJECT_ID"] = OBJECT_ID

        obj_list_encoded.append(self.write_datum(self.gis_point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})


    def add_point(self, set_id, x, y, OBJECT_ID=''):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        obj_list_encoded = []

        datum = collections.OrderedDict()
        datum['x'] = x
        datum['y'] = y
        datum['OBJECT_ID'] = OBJECT_ID

        obj_list_encoded.append(self.write_datum(self.point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})

    # This assumes equal length lists
    def bulk_add_big_point(self, set_id, msg_id_list, x_list, y_list, timestamp_list, source_list, group_id_list, OBJECT_ID_list=None):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        if (OBJECT_ID_list is None):
            OBJECT_ID_list = ['' for x in x_list]

        obj_list_encoded = []

        for msg_id,x,y,timestamp,source,group_id,object_id in zip(msg_id_list,x_list,y_list,timestamp_list,source_list,group_id_list,OBJECT_ID_list):
            datum = collections.OrderedDict()
            datum['msg_id'] = msg_id
            datum['x'] = x
            datum['y'] = y
            datum['TIMESTAMP'] = timestamp
            datum['source'] = source
            datum['group_id'] = group_id
            datum['OBJECT_ID'] = object_id
            obj_list_encoded.append(self.write_datum(self.big_point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})

    # This assumes that 'x' and 'y' are equal length lists
    def bulk_add_point(self, set_id, x_list, y_list, OBJECT_ID_list=None):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        if (OBJECT_ID_list is None):
            OBJECT_ID_list = ['' for x in x_list]

        obj_list_encoded = []

        for i in range(0,len(x_list)):
            datum = collections.OrderedDict()
            datum['x'] = x_list[i]
            datum['y'] = y_list[i]
            datum['OBJECT_ID'] = OBJECT_ID_list[i]
            obj_list_encoded.append(self.write_datum(self.point_schema, datum))

        return self.insert_records(set_id, obj_list_encoded, None, {"return_record_ids":"true"})

    # Helper function to emulate old /add (single object insert) capability
    def insert_object(self, set_id, object_data, params=None):
        if (params):
            return self.insert_records(set_id, [object_data], None, params)
        else:
            return self.insert_records(set_id, [object_data], None, {"return_record_ids":"true"})

    # Helper for dynamic schema responses
    def parse_dynamic_response(self, retobj, do_print=False):

        if (retobj['status_info']['status'] == 'ERROR'):
            print 'Error: ', retobj['status_info']['message']
            return retobj

        if len(retobj['binary_encoded_response']) > 0:
  
            my_schema = schema.parse(retobj['response_schema_str'])
            
            csio = cStringIO.StringIO(retobj['binary_encoded_response'])
            bd = io.BinaryDecoder(csio)
            reader = io.DatumReader(my_schema)
            decoded = reader.read(bd) # read, give a decoder

            #translate the column names
            column_lookup = decoded['column_headers']

            translated = collections.OrderedDict()
            for i,column_name in enumerate(column_lookup):
                translated[column_name] = decoded['column_%d'%(i+1)]

            retobj['response'] = translated
        else:
            retobj['response'] = collections.OrderedDict()

            #note running eval here returns a standard (unordered) dict
            d_resp = eval(retobj['json_encoded_response'])

            #now go through the fields in order according to the schema
            my_schema = schema.parse(retobj['response_schema_str'])

            column_lookup = d_resp['column_headers']

            for i,column_name in enumerate(column_lookup):
                retobj['response'][column_name] = d_resp['column_%d'%(i+1)]

        if (do_print):
            print tabulate(retobj['response'],headers='keys',tablefmt='psql')

        return retobj

    # ------------- END convenience functions ------------------------------------


    # ------------- BEGIN functions for GPUdb developers -----------------------

    # -----------------------------------------------------------------------
    # join -> /join

    def join(self, left_set, left_attr, right_set, right_attr, result_type, result_set, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["left_attr"] = left_attr
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["result_type"] = result_type
        datum["result_set"] = result_set
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/join")

    # -----------------------------------------------------------------------
    # join_incremental -> /joinincremental

    def join_incremental(self, left_subset, left_attr, left_index, right_set, right_attr, result_set, result_type, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join_incremental")

        datum = collections.OrderedDict()
        datum["left_subset"] = left_subset
        datum["left_attr"] = left_attr
        datum["left_index"] = left_index
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["result_set"] = result_set
        datum["result_type"] = result_type
        datum["data_map"] = {}
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinincremental")

    # -----------------------------------------------------------------------
    # join_setup -> /joinsetup

    #initial join setup for the incremental join
    def join_setup(self, left_set, left_attr, right_set, right_attr, subset_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join_setup")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["left_attr"] = left_attr
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["subset_id"] = subset_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinsetup")


    # -----------------------------------------------------------------------
    # predicate_join -> /predicatejoin

    def predicate_join(self, left_set, right_set, predicate, common_type, result_type, result_set, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("predicate_join")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["right_set"] = right_set
        datum["common_type"] = common_type
        datum["result_type"] = result_type
        datum["result_set"] = result_set
        datum["user_auth_string"] = user_auth
        datum["predicate"] = predicate

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/predicatejoin")


    # -----------------------------------------------------------------------
    # register_type_transform -> /registertypetransform

    def register_type_transform(self, type_id, new_type_id, transform_map):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type_transform")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["new_type_id"] = new_type_id
        datum["transform_map"] = transform_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertypetransform")

    # ------------- END functions for GPUdb developers -----------------------



    # -----------------------------------------------------------------------
    # Begin autogenerated functions
    # -----------------------------------------------------------------------

    # @insert_autogen

    # -----------------------------------------------------------------------
    # End autogenerated functions
    # -----------------------------------------------------------------------


# end class GPUdb


