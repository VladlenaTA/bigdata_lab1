#!/bin/bash

rm -r output
hdfs dfs -rm -r lab1

hdfs dfs -mkdir lab1
hdfs dfs -put input lab1/input

hadoop jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar lab1/input lab1/output

hdfs dfs -get lab1/output output
cat output/*
