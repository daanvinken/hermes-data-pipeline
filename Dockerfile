FROM ubuntu:18.04

# install and setup docker
RUN apt-get update

RUN apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg-agent \
  software-properties-common

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -

RUN add-apt-repository \
  "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) \
  stable"

RUN apt-get update

RUN apt-get install -y docker-ce docker-ce-cli containerd.io

VOLUME /var/lib/docker

ENTRYPOINT ["dockerd"]

# install jdk
RUN apt-get install -y openjdk-8-jdk openjdk-8-demo openjdk-8-doc \
  openjdk-8-jre-headless openjdk-8-source

# install and set up sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkps://keyserver.ubuntu.com:443 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update
RUN apt-get install -y sbt

WORKDIR /root

RUN mkdir project && \
  echo 'scalaVersion := "2.12.9"' > build.sbt && \
  echo 'sbt.version=1.3.3' > project/build.properties && \
  echo 'case object Temp' > Temp.scala && \
  sbt compile && \
  rm -r project && rm build.sbt && rm Temp.scala && rm -r target
