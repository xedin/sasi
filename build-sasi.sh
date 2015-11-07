rm -rf cassandra-trunk
git clone git@github.com:apache/cassandra.git cassandra-trunk
cd cassandra-trunk
for i in $(ls ../patches | sort)
do
  git am $(cd $(dirname "$i") && pwd -P)/../patches/$(basename "$i")
done
#cp -R ../src/* src/
#cp -R ../test/* test/
rm -r interface/thrift/gen-java/*
ant realclean
ant gen-thrift-java
ant