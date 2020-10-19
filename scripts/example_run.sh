#!/usr/bin/env bash

java -version
hadoop=/usr/lib/hadoop-3.2.1
class=be.ugent.intec.ddecap.BlsSpeller
input=src/test/resources/groupOrtho1.txt
output="${1:-output}"
bindir=../suffixtree-motif-speller/motifIterator/build

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$hadoop/lib/native HADOOP_HOME=$hadoop \
spark-submit \
--conf spark.ui.showConsoleProgress=true --driver-memory=6g --executor-memory 2g --num-executors 1 \
--name BLSSpeller \
--class $class \
target/scala-2.11/bls-speller-assembly-0.1.jar \
--AB \
-i "$input" \
-o "$output" \
-b "$bindir" \
-p 2 \
--alphabet 1 \
--degen 1 \
--min_len 7 \
--max_len 8 \
--logging_mode spark_measure \
