#Install Mongo C Driver

sudo yum install -y git gcc automake autoconf libtool
git clone https://github.com/mongodb/mongo-c-driver
cd mongo-c-driver

./autogen.sh
make
make install


 git clone https://github.com/johnlpage/WorkLoad
 cd Workload
 make
 