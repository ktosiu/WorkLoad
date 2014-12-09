pkill mongod
pkill mongos


/bin/rm -rf /data/config
mkdir -p /data/config
mkdir /data/logs

for s in 1 2 3 4
do
 rm -rf /data/shard$s
 mkdir /data/shard$s
 /home/ec2-user/mongodb-linux-x86_64-2.6.5/bin/mongod --port 2702$s --dbpath /data/shard$s --logpath /data/logs/mongod-$s-shard.log --fork --syncdelay=2
 sleep 1
done


 /home/ec2-user/mongodb-linux-x86_64-2.6.5/bin/mongod --port 27019 --configsvr --dbpath /data/config --logpath /data/logs/config.log --fork
 
  /home/ec2-user/mongodb-linux-x86_64-2.6.5/bin/mongos --configdb '10.10.2.89:27019' --logpath /data/logs/mongos.log --fork --port 27017
    
    
    /home/ec2-user/mongodb-linux-x86_64-2.6.5/bin/mongo
    
    sh.addShard('10.9.137.116:27021')
    sh.addShard('10.9.137.116:27022')
    sh.addShard('10.9.137.116:27023')
    sh.addShard('10.9.137.116:27024')
    
     sh.enableSharding('testsrv')
    sh.shardCollection('testsrv.data',{_id:1})
    
    
    
    
    