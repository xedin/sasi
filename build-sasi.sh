rm -rf cassandra-2.0.17
git clone --branch cassandra-2.0.17 git@github.com:apache/cassandra.git cassandra-2.0.17
cd cassandra-2.0.17
for i in $(ls ../patches | sort)
do
  git am $(cd $(dirname "$i") && pwd -P)/../patches/$(basename "$i")
done
cp -R ../src/* src/
cp -R ../test/* test/
rm -r interface/thrift/gen-java/*
ant realclean
ant gen-thrift-java
ant