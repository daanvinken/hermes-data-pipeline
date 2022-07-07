#!/bin/bash
SPARK_RELEASE=spark-2.4.8-bin-hadoop2.7
HOST_ADDR=$(hostname --all-ip-address|cut -d ' ' -f1)

# Install Java 11
sudo yum -y install java-8-openjdk

echo "Installing spark..."
cd /home/vagrant || exit 1

wget https://dlcdn.apache.org/spark/spark-2.4.8/${SPARK_RELEASE}.tgz
tar xvf ${SPARK_RELEASE}.tgz
rm -rf ${SPARK_RELEASE}.tgz
mv ${SPARK_RELEASE} spark
cd spark || exit 1

sudo rm -f /etc/yum.repos.d/bintray-rpm.repo
curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
sudo mv sbt-rpm.repo /etc/yum.repos.d/
sudo dnf install sbt

# run spark
cd /home/vagrant/spark/sbin
sudo ./start_master.sh
sudo ./start-slave.sh spark://${HOST_ADDR}:7077