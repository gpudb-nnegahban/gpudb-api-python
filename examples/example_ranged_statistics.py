from gaia import Gaia
import uuid
import random

def print_stats(retobj):
    stats = retobj["stats"]
    count = stats["count"]
    sumx  = stats["sum"]
    mean  = stats["mean"]
    stdv  = stats["stdv"]
    variance = stats["variance"]
    skew  = stats["skew"]
    stdv  = stats["stdv"]
    kurtosis = stats["kurtosis"]

    print "%3s: %10s, %10s, %10s, %10s, %10s, %10s"%("i","count","sum","mean","stdv","skew","kurtosis")
    for i in range(len(count)):
        print "%3d: %10.1f, %10.1f, %10.1f, %10.1f, %10.3f, %10.3f"%(i,count[i],sumx[i],mean[i],stdv[i],skew[i],kurtosis[i])

def example_ranged_statistics():
    gaiadb = Gaia( encoding='BINARY', gaia_ip='127.0.0.1', gaia_port='9191' )

    # need a type first; using the "big" point type
    retobj = gaiadb.register_type_big_point()    
    type_id = retobj['type_id']

    # create a new set
    set_id = str(uuid.uuid1())
    retobj = gaiadb.new_set(type_id = type_id, set_id = set_id)
    print retobj

    # add 1000 points to the set 
    N = 1000
    interval = N/20.0
    for i in range(N):
        x = random.uniform(0,i)
        y = x*x
        timestamp = i
        gaiadb.add_big_point(set_id, "MSGID1", x, y, timestamp, "SRC1", "GROUP1")

    print
    print 'ranged_statistics on TIMESTAMP, value attribute x'
    print 'for TIMESTAMP in range [0,',N,'] divided into intervals of size',interval
    retobj = gaiadb.ranged_statistics(set_id = set_id, attribute = "TIMESTAMP", value_attribute = "x", stats = "sum,mean,stdv,variance,skew,kurtosis", interval = interval, start = 0, end = N);
    print_stats(retobj)

    print
    print 'ranged_statistics on TIMESTAMP, value attribute y'
    retobj = gaiadb.ranged_statistics(set_id = set_id, attribute = "TIMESTAMP", value_attribute = "y", stats = "sum,mean,stdv,variance,skew,kurtosis", interval = interval, start = 0, end = N);
    print_stats(retobj)

    print
    print 'range histogram on TIMESTAMP, value attribute y, with additional_attributes = x'
    print 'count and sum should be sum of previous two stats printouts'
    params = {"additional_attributes" : "x" }
    retobj = gaiadb.ranged_statistics(set_id = set_id, attribute = "TIMESTAMP", value_attribute = "y", stats = "sum,mean,stdv,variance,skew,kurtosis", interval = interval, start = 0, end = N, select_expression = "", params = params);
    print_stats(retobj)

    print
    select_expression = "TIMESTAMP <" + str(N) + "/2"
    print 'ranged_statistics on TIMESTAMP, value attribute x with select_expression =',select_expression
    print 'bins 10-19 should have counts of 0'
    print "select_expression =",select_expression
    retobj = gaiadb.ranged_statistics(set_id = set_id, attribute = "TIMESTAMP", value_attribute = "x", stats = "sum,mean,stdv,variance,skew,kurtosis", interval = interval, start = 0, end = N, select_expression = select_expression);
    print_stats(retobj)

    print
    select_expression = "x <" + str(N) + "/2"
    print 'ranged_statistics on TIMESTAMP, value attribute x with select_expression =',select_expression
    print 'bins 10-19 counts values should be decreasing'
    retobj = gaiadb.ranged_statistics(set_id = set_id, attribute = "TIMESTAMP", value_attribute = "x", stats = "sum,mean,stdv,variance,skew,kurtosis", interval = interval, start = 0, end = N, select_expression = select_expression);
    print_stats(retobj);
        

if __name__ == '__main__':
   example_ranged_statistics()
