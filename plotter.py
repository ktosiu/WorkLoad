from pylab import *
import csv

darray = {}
fname = sys.argv[1]

with open(fname, 'rb') as infile:
    reader = csv.reader(infile, delimiter=',', quotechar='"', skipinitialspace=True )
    for parts in reader:
        fno=0
        for f in parts:
            try:
                darray[fno].append(int(f))
            except KeyError:
                darray[fno]=[int(f)]
            fno=fno+1

vmax = max( [  max(darray[7]), max(darray[13]), max(darray[19]) ] )
vmax=vmax*1.6
gca().get_yaxis().get_major_formatter().set_scientific(False)
gca().get_xaxis().get_major_formatter().set_scientific(False)
plot(darray[0],darray[4],label='Insert Mean')
plot(darray[0],darray[7],label='Insert 90%')

plot(darray[0],darray[10],label='Update Mean')
plot(darray[0],darray[13],label='Update 90%')

plot(darray[0],darray[16],label='Query Mean')
plot(darray[0],darray[19],label='Query 90%')
ylabel('ms/op')
legend()
title("Mean and 90th centile")
xlabel('Seconds elapsed')
axis([0,max(darray[0]),0,vmax])
savefig(fname+'.latency.png',dpi=300)

close()

bysec = lambda x: x/60
insbysec = map(bysec,darray[2]);
uptbysec = map(bysec,darray[8]);
qrybysec = map(bysec,darray[14]);
vmax = max( [  max(insbysec),max(uptbysec),max(qrybysec) ] )
print vmax
gca().get_xaxis().get_major_formatter().set_scientific(False)
gca().get_yaxis().get_major_formatter().set_scientific(False)
plot(darray[0],insbysec,label="Insert")
plot(darray[0],uptbysec,label="Update")
plot(darray[0],qrybysec,label="Query")
ylabel('Ops per second')
legend()
title("Operations per Second")
xlabel('Seconds elapsed')
axis([0,max(darray[0]),0,vmax])
savefig(fname+'.ops.png',dpi=300)
close()


vmax = max(darray[1])
gca().get_xaxis().get_major_formatter().set_scientific(False)
plot(darray[0],darray[1])
gca().get_yaxis().get_major_formatter().set_scientific(False)
ylabel('Records in DB')
axis([0,max(darray[0]),0,vmax])
title(fname)
xlabel('Seconds elapsed')
title("Total Data Volume")
savefig(fname+'.numrecs.png',dpi=300)
close()

# show()

gca().get_xaxis().get_major_formatter().set_scientific(False)
vmax = max( [  max(darray[5]), max(darray[11]), max(darray[17]) ] )
gca().get_yaxis().get_major_formatter().set_scientific(False)
plot(darray[0],darray[5],label="Insert")

plot(darray[0],darray[11],label="Update")

plot(darray[0],darray[17],label="Query")
title("Maximum time to complete op")
ylabel('ms/op')
legend()
axis([0,max(darray[0]),0,vmax])
xlabel('Seconds elapsed')
savefig(fname+'.max.png',dpi=300)
close();

