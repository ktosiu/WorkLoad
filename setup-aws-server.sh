#!/bin/sh

VERSION=mongodb-linux-x86_64-2.8.0-rc1
export VERSION

#Create a RAID Array from the Disks
#This gives WT and MMAP Equal chances
umount /dev/xvdb
umount /dev/xvdc


mdadm --create --verbose /dev/md/raid --level=stripe --raid-devices=2 /dev/xvdb /dev/xvdc
mkfs.ext4 /dev/md/raid

blockdev --setra 32 /dev/md/raid 

mkdir /data
mount /dev/md/raid /data

wget https://www.mongodb.org/dr//fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.8.0-rc1.tgz/download

mv download mongodb28.tgz

tar -xvzf mongodb28.tgz
	
#Setup simple single MongoD MMAP V1 Instance


mkdir /data/db-mmap
mkdir /data/logs


/home/ec2-user/mongodb-linux-x86_64-2.8.0-rc1/bin/mongod --dbpath /data/db-mmap --logpath /data/logs/mmapv1.log --fork



