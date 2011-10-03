#!/bin/sh

java -cp target/BloomFilterExample-1.0-jar-with-dependencies.jar:/etc/hadoop/conf/ com.busywait.bloomfilterexample.BloomViaConfiguration $@