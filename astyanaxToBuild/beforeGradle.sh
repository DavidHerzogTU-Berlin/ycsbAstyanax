#!/bin/bash
echo "delete .m2/repository/com/jcraft/"
rm -r ~/.m2/repository/com/jcraft/
echo "delete .m2/repository/com/google/code/findbugs/jsr305/"
rm -r ~/.m2/repository/com/google/code/findbugs/jsr305/
./gradlew build -x test
echo "copy astyanax-core to maven repo. and replace old version"
cp astyanax-core/build/libs/astyanax-core-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-core/1.56.42
rm ~/.m2/repository/com/netflix/astyanax/astyanax-core/1.56.42/astyanax-core-1.56.42.jar
mv ~/.m2/repository/com/netflix/astyanax/astyanax-core/1.56.42/astyanax-core-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-core/1.56.42/astyanax-core-1.56.42.jar 

echo "copy astyanax-thrift to maven repo. and replace old version"
cp astyanax-thrift/build/libs/astyanax-thrift-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-thrift/1.56.42
rm ~/.m2/repository/com/netflix/astyanax/astyanax-thrift/1.56.42/astyanax-thrift-1.56.42.jar
mv ~/.m2/repository/com/netflix/astyanax/astyanax-thrift/1.56.42/astyanax-thrift-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-thrift/1.56.42/astyanax-thrift-1.56.42.jar

echo "copy astyanax-cassandra to maven repo. and replace old version"
cp astyanax-cassandra/build/libs/astyanax-cassandra-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-cassandra/1.56.42
rm ~/.m2/repository/com/netflix/astyanax/astyanax-cassandra/1.56.42/astyanax-cassandra-1.56.42.jar
mv ~/.m2/repository/com/netflix/astyanax/astyanax-cassandra/1.56.42/astyanax-cassandra-2.0.2-SNAPSHOT.jar ~/.m2/repository/com/netflix/astyanax/astyanax-cassandra/1.56.42/astyanax-cassandra-1.56.42.jar 