#Think I hit the limit of the aggregation pipeline


import pymongo


#Connect to local mongoDB

connection_string = "mongodb://54.170.164.54:27017"
connection = pymongo.MongoClient(connection_string)
database = connection.testresults
collection  = database.results

tops = {}
ttime = {}
longesttime = {}
longops = {}
seconds = -1
nrecs = 0
times = {}
optypes = ('insert','updates','queries')

class GrowingList(list):
    def __setitem__(self, index, value):
        if index >= len(self):
            self.extend([None]*(index + 1 - len(self)))
        list.__setitem__(self, index, value)
        
#We have one result per thread per minute
def output():
    if seconds <1:
        return
    print(str(seconds) + ","),
    print(str(nrecs)),
    for op in optypes:
        print ("," + str(tops[op]) + ", " + str(ttime[op]) + ", " + str(ttime[op]/tops[op])),
        print ("," + str(longesttime[op]) + ", " + str(longops[op]) ),
        #Work out 90th centile
        totalops = tops[op]
        nintyth = totalops * 0.90
        adops = 0
        centiletime = 0
        for ct in times[op]:
            centiletime = centiletime + 10
            adops = adops + ct
            if adops > nintyth:
                break
        print ("," + str(centiletime)),
            
                
            
    print
        
collection.ensure_index([("_id.seconds",1)])
results = collection.find().sort([("_id.seconds",1)])

slicesize = 10;



for result in results:
    timeslice = int(result['_id']['seconds']  / slicesize) * slicesize;
    if timeslice != seconds:
        output()
        for op in optypes:
            tops[op] = 0
            ttime[op] = 0
            longesttime[op] = 0
            longops[op] = 0
            times[op] = GrowingList()
    
    seconds = timeslice;
    nrecs = result['nrecs']
    for op in ('insert','updates','queries'):
        sresult = result[op]
        tops[op] = tops[op] + sresult['total_ops'];
        ttime[op] = ttime[op] + sresult['total_time'];
        if sresult['longest_time'] > longesttime[op]:
            longesttime[op] = sresult['longest_time']
        longops[op] = longops[op] + sresult['long_ops']
        tcount = 0
        for t in sresult['times']:
            try:
                times[op][tcount] = times[op][tcount] + t
            except IndexError:
                times[op][tcount] = t
            tcount=tcount+1

    


