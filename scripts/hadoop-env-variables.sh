#!/bin/bash
export JAVA_HOME=$(readlink -n \/etc\/alternatives\/java | sed "s:\/bin\/java::")
echo "JAVA_HOME: " $JAVA_HOME
export HADOOP_HOME=/usr/local/hadoop/home
echo "HADOOP_HOME: " $HADOOP_HOME
export HADOOP_LIB="$HADOOP_HOME/lib/native/"
echo "HADOOP_LIB: " $HADOOP_LIB
ls $JAVA_HOME/jre/lib/amd64/server/
export LD_LIBRARY_PATH="$HADOOP_LIB:$JAVA_HOME/lib/amd64/server/"
echo "LD_LIBRARY_PATH: " $LD_LIBRARY_PATH
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
echo "CLASSPATH: " $CLASSPATH


