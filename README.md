# MyHDFS
This project is a minimal implementation of distributed file system similar to HDFS

### Features

    * Uses Java RMI + Google protobuf for communication
    * Replication factor = 2 
    * Block size = 64K 
    * NameNode & DataNodes maintains the information of block report to recover even after restart of machine
    * Supports 4 DataNodes running on different machines interconnected

### Running NameNode & DataNode

    * Provide the datanode IPs in config.properties and run namenode as "java -jar NameNode.jar"
    * Run datanode as "java -jar -Djava.rmi.server.hostname=<NameNodeIP> dataNode.jar"
    
### Protobuf Generation Command

    * protoc -I<include-path> --java_out=<src-dir> <.proto file path>
