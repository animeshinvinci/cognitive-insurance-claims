# Installing Spark Jobserver on IBM BigInsights in Bluemix
## High level steps
1. Deploy a new instance of BigInsights service in Bluemix
2. Create a new cluster including Spark as an optional add-on.
3. SSH into the master
4. Prepare the server
5. Build SJS
6. Create SSH Tunnel
7. Test

## Deploy BigInsights in Bluemix
```bash
cf create-service BigInsightsonCloud "Basic (Beta)" "my_biginsights"
```
After deploying a service instance you must log into the Bluemix console and deploy a Hadoop cluster.  You must remember the username and password you provide for the master server as you will them to SSH into the master. 

## Prepare Master Machine for Spark Jobserver
First you must ssh into the master and set update the `PATH` with your user's home directory.
```bash
mkdir bin
vi .profile
  PATH=$HOME/bin:$PATH
source .profile
```
### Install autoconf
```bash
mkdir src
wget https://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
tar zxvf autoconf*
cd autoconf-2.69
./configure --prefix=$HOME && make && make install
```
### Install Git
http://joemaller.com/908/how-to-install-git-on-a-shared-host/
```bash
cd ~/src
wget https://curl.haxx.se/download/curl-7.47.1.tar.gz
tar zxvf curl*
curl -LO https://github.com/git/git/tarball/v2.9.2
tar zxvf v2.9.2
cd git-git-e6eeb1a/
make configure
./configure --prefix=$HOME --with-curl=~/src/curl-7.47.1
make && make install
```

### Install sbt
Follow the instructions on the main [sbt](http://www.scala-sbt.org/0.13/docs/Manual-Installation.html) site to install sbt.
```bash
cd ~/bin
wget https://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.12/sbt-launch.jar
vi ~/bin/sbt
  #!/bin/bash
  SBT_OPTS="-Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256M"
  java $SBT_OPTS -jar `dirname $0`/sbt-launch.jar "$@"
chmod u+x ~/bin/sbt
```

### Create and add your GitHub SSH Key
Follow the steps on [GitHub](https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/) to generate and add an SSH key to your GitHub account.  You will use this to clone Spark Jobserver.

## Build and install Spark-jobserver
Build SJS by running:
```bash
cd ~/src
git clone git@github.com:spark-jobserver/spark-jobserver.git
cd spark-jobserver
git checkout v0.6.2
#Temporary fix since this class does not compile on Java 8
rm job-server-tests/src/main/java/spark.jobserver/JavaHelloWorldJob.java
sbt clean update package assembly
mkdir log
```
Now you need to create the proper configuration for utilizing your BigInsights environment.
```
vi config/bi.sh
```
Then add the following lines, replacing the values for `APP_USER`, `INSTALL_DIR`, and `LOG_DIR` with your BigInsights username.
```bash
APP_USER=hadoop
APP_GROUP=biusers
INSTALL_DIR=/home/hadoop/sjs
LOG_DIR=/home/hadoop/log/sjs
PIDFILE=spark-jobserver.pid
JOBSERVER_MEMORY=1G
SPARK_VERSION=1.6.1
SPARK_HOME=/usr/iop/4.2.0.0/spark
SPARK_CONF_DIR=/etc/spark/conf
HADOOP_CONF_DIR=/etc/hadoop/conf
YARN_CONF_DIR=/etc/hadoop/conf
SCALA_VERSION=2.10.5
```
Now create your conf file:
```
vi config/bi.conf
```
and add the following:
```
spark {
 # spark.master will be passed to each job's JobContext
master = "yarn-client"
jobserver {
 port = 8090
 jobdao = spark.jobserver.io.JobFileDAO
 filedao {
   rootdir = /tmp/spark-jobserver/filedao/data
 }
}
# predefined Spark contexts
contexts {
 # test {
 #   num-cpu-cores = 1            # Number of cores to allocate.  Required.
 #   memory-per-node = 1g         # Executor memory per node, -Xmx style eg 512m, 1G, etc.
 #   spark.executor.instances = 1
 # }
 # define additional contexts here
}
# universal context configuration.  These settings can be overridden, see README.md
context-settings {
 num-cpu-cores = 4          # Number of cores to allocate.  Required.
 memory-per-node = 4g         # Executor memory per node, -Xmx style eg 512m, #1G, etc.
 spark.executor.instances = 4
 # If you wish to pass any settings directly to the sparkConf as-is, add them here in passthrough,
 # such as hadoop connection settings that don't use the "spark." prefix
 passthrough {
   #es.nodes = "192.1.1.1"
 }
}
# This needs to match SPARK_HOME for cluster SparkContexts to be created successfully
home = "/usr/iop/current/spark""
}
```
Finally, package and deploy your custom Spark Jobserver:
```bash
bin/server_package.sh bi
mkdir /home/hadoop/sjs
cd /home/hadoop/sjs
tar zxf /tmp/job-server/job-server.tar.gz
./server_start.sh &
```
## Create tunnel
Unfortunately Spark Jobserver's port, `8090`, is not open through the Bluemix firewall.  You can get around this restriction by deploying an SSH tunnel in Bluemix which acts as a secure proxy to expose SJS.  I have created such a proxy at [biginsights_ssh_tunnel](../biginsights_ssh_tunnel).

Modify `index.js` and replace `host`, `username`, and `password` with your BigInsights hostname, username, and password before running `cf push` in the [biginsights_ssh_tunnel](../biginsights_ssh_tunnel) directory.  You may also need to change the `host` attribute in the [manifest.yml](../biginsights_ssh_tunnel/manifest.yml).

## Try it out
Once you've deployed your SSH tunnel you can test the configuration on your local machine by running the following:
```
cd ~/workspaces/bpmNext/SparkJobServer
curl --data-binary @target/scala-2.10/SparkJobServer-assembly-1.0.jar http://cogclaimbiproxy.mybluemix.net/jars/test 
curl -d "" 'http://cogclaimbiproxy.mybluemix.net/contexts/model-context'
curl -d "input.modelType=decisionForest" 'http://cogclaimbiproxy.mybluemix.net/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.SparkModelJob&context=model-context'
curl -d "input.approvedAmount=1000, input.estimate=2000, input.creditScore=850" 'http://cogclaimbiproxy.mybluemix.net/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.PredictClaimJob&context=model-context&sync=true' 
```
