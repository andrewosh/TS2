start-hbase.sh
hbase-daemon.sh start thrift
hdfs dfs -mkdir /home/
hdfs dfs -mkdir /home/$USER
hdfs dfs -put /home/$USER/hbase /home/$USER

