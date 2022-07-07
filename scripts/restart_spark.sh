#!/bin/bash

export HOST_ADDR=$(hostname --all-ip-address|cut -d ' ' -f1)

cd /home/vagrant/spark/sbin || exit
sudo ./stop-master.sh
sudo ./stop-slave.sh
sudo ./start-master.sh
sudo ./start-slave.sh spark://${HOST_ADDR}:7077
cd -