#!/usr/bin/env bash

set -e
java -version

class=be.ugent.intec.ddecap.BlsSpeller
input=src/test/resources/groupOrtho1.txt
bindir=../suffixtree-motif-speller/motifIterator/build
jar=target/scala-2.12/bls-speller-assembly-0.1.jar

output="${1:-output}"
event_log_dir="$output/eventlogdir"
metrics_dir="$output/metrics"

rm -rf "$output"
mkdir -p "$output" "$metrics_dir" "$event_log_dir"

metrics=$(tr '\n' ' ' <<EOF
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink
--conf spark.metrics.conf.*.sink.csv.period=10
--conf spark.metrics.conf.*.sink.csv.unit=seconds
--conf spark.metrics.conf.*.sink.csv.directory=$metrics_dir
--conf spark.metrics.conf.*.source.jvm.class=org.apache.spark.metrics.source.JvmSource
--conf spark.executor.processTreeMetrics.enabled=true
EOF
)

eventlog=$(tr '\n' ' ' <<EOF
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=$event_log_dir
--conf spark.eventLog.logStageExecutorMetrics.enabled=true
EOF
)

conf=$(tr '\n' ' ' <<EOF

EOF
)

cmdargs=$(tr '\n' ' ' <<EOF
--AB
-i $input
-o $output
-b $bindir
-p 2
--alphabet 1
--degen 1
--min_len 7
--max_len 8
EOF
)

spark-submit $metrics $eventlog $conf --name BLSSpeller --class $class $jar $cmdargs
