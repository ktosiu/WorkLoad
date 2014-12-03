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


plot(darray[0],darray[2])
show()