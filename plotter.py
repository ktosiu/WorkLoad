from pylab import *
import csv

darray = {}

with open('stats.csv', 'rb') as infile:
    reader = csv.reader(infile, delimiter=',', quotechar='"', skipinitialspace=True )
    for parts in reader:
        fno=0
        for f in parts:
            try:
                darray[fno].append(int(f))
            except KeyError:
                darray[fno]=[int(f)]
            fno=fno+1

vmax = max( [  max(darray[5]), max(darray[11]), max(darray[17]) ] )

plot(darray[0],darray[4])
plot(darray[0],darray[5])
plot(darray[0],darray[7])

plot(darray[0],darray[10])
plot(darray[0],darray[11])
plot(darray[0],darray[13])

plot(darray[0],darray[16])
plot(darray[0],darray[17])
plot(darray[0],darray[19])

axis([0,max(darray[0]),0,vmax])
show()