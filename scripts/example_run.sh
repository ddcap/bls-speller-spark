#!/bin/bash

hadoop=/usr/lib/hadoop-3.2.1
class=be.ugent.intec.ddecap.BlsSpeller
input=../suffixtree-motif-speller/suffixtree/groupOrtho1.txt
output=${1:=blstest}
bindir=../suffixtree-motif-speller/suffixtree

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$hadoop/lib/native HADOOP_HOME=$hadoop \
spark-submit --conf spark.ui.showConsoleProgress=true --executor-memory 3g --num-executors 1 \
--name BLSSpeller \
--class $class \
target/scala-2.11/bls-speller-assembly-0.1.jar \
--AB \
-i $input \
-o $output \
-b $bindir \
-p 2 \
--alphabet 1 \
--degen 1 \
--min_len 7 \
--max_len 8 \
