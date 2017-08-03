#!/bin/bash

function update_apt_repo  {
  sudo apt-get purge -y openjdk*
  sudo add-apt-repository -y ppa:webupd8team/java
  sudo apt-get update -y
} 
function install_java {
  sudo apt-get install -y oracle-java8-installer
  sudo apt-get install -y oracle-java8-set-default
}

function install_hadoop {
  sudo mkdir -p /usr/local/hadoop/
  cd /usr/local/hadoop
  sudo curl http://ftp.tc.edu.tw/pub/Apache/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz | sudo tar xz 
  sudo chown -R hadoop /usr/local/hadoop
}

function create_hadoop_user {
  sudo useradd -m hduser
  sudo adduser hduser sudo
  sudo chsh -s /bin/bash hduser
  echo -e "hduser123\nhduser123\n" | sudo passwd hduser

  sudo useradd -m hadoop
  sudo adduser hadoop sudo
  sudo chsh -s /bin/bash hadoop
  echo -e "hadoop123\nhadoop123\n" | sudo passwd hadoop
}

function setup_profile {
  local file=/etc/profile.d/hadoop-init.sh
  local tempfile=/tmp/hadoop_setup_sdfds.sh
  sudo mkdir -p /tmp/hadoop
  sudo chown hduser -R /tmp/hadoop
  export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
  export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
  cat >> $tempfile  <<EOT
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
EOT
  tempfile=/tmp/hadoop_setup_sdfds.sh
  chmod +x $tempfile
  sudo chown root $tempfile
  sudo mv $tempfile $file
}

function setup_core_xml {
  export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
  local tmpfile=/tmp/hadoop_fafsa.xml
  local file=$HADOOP_HOME/etc/hadoop/core-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadooop</value>
    <description>Temporary directories.</description>
</property>
<property>
    <name>fs.default.name</name>
    <value>hdfs://localhost:54310</value>
    <description>A URI whose scheme and authority determine the FileSystem implementation. </description>
</property>
</configuration>
EOF
  tmpfile=/tmp/hadoop_fafsa.xml
  sudo chown root $tmpfile
  sudo mv $tmpfile $file
}

function setup_mapred_xml {
  export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
  local tmpfile=/tmp/hadoop_mapred.xml
  local file=$HADOOP_HOME/etc/hadoop/mapred-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOT
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>mapred.job.tracker</name>
  <value>localhost:54311</value>
  <description>The tracker of MapReduce</description>
</property>
</configuration>
EOT
  tmpfile=/tmp/hadoop_mapred.xml
  sudo chown root $tmpfile
  sudo mv $tmpfile $file
}

function setup_hdfs_xml {
  export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
  local tmpfile=/tmp/hadoop_hdfs.xml
  local file=$HADOOP_HOME/etc/hadoop/hdfs-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>dfs.replication</name>
  <value>1</value>
  <description>Number of replication of hdfs</description>
</property>
</configuration>
EOF
  tmpfile=/tmp/hadoop_hdfs.xml
  sudo chown root $tmpfile
  sudo mv $tmpfile $file
}


function setup_environment {
  export HADOOP_HOME=/usr/local/hadoop/hadoop-2.7.2
  sudo sed -i -- 's/JAVA_HOME=\${JAVA_HOME}/JAVA_HOME=\$(readlink -f \/usr\/bin\/java | sed "s:bin\/java::")/' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
  setup_profile
  setup_core_xml
  setup_mapred_xml
  setup_hdfs_xml
  sudo chown -R hduser $HADOOP_HOME
}

function start-all {
  sudo $HADOOP_HOME/bin/hdfs namenode -format
  sudo mkdir /root/.ssh
  sudo ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa
  sudo cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
  ssh-keyscan -H 127.0.0.1 >> /root/.ssh/known_hosts
  ssh-keyscan -H 0.0.0.0 >> /root/.ssh/known_hosts
  ssh-keyscan -H localhost >> /root/.ssh/known_hosts
  sudo $HADOOP_HOME/sbin/start-dfs.sh
}

update_apt_repo 
install_java
create_hadoop_user
install_hadoop 
setup_environment
start-all
