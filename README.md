# bls-speller-spark

## Installation

- install [Apache Spark](https://spark.apache.org/)
- install [sbt](https://www.scala-sbt.org/)
- build a [suffixtree](https://bitbucket.org/dries_decap/suffixtree-motif-speller/src/master/) executable using `make`
- build bls-speller-spark with
```
sbt assembly
```

## Usage

- have a groupOrtho input txt file e.g. `suffixtree-motif-speller/suffixtree/groupOrtho1.txt`
- run `spark-submit` with the right parameters, see `example_run.sh`
```
bash ./scripts/example_run.sh
```

```
Usage: bls speller [options]

  -i, --input <value>      Location of the input files.
  -o, --output <value>     The output directory (spark output) or VCF output file (merged into a single file).
  -b, --bindir <value>     Location of the directory containing all binaries.
  -p, --partitions <value>
                           Number of partitions used by executors.
  --alphabet <value>       Sets the alphabet used in motif iterator: 0: Exact, 1: Exact+N, 2: Exact+2fold+M, 3: All. Default is 2.
  --degen <value>          Sets the max number of degenerate characters.
  --min_len <value>        Sets the minimum length of a motif.
  --max_len <value>        Sets the maximum length of a motif, this is not inclusive (i.e. length < maxLength).
  --persist_level <value>  Sets the persist level for RDD's: mem, mem_ser, mem_disk, mem_disk_ser, disk [default]
  --AB                     Alignment based motif discovery.
```

## Profiling

### [Flamegraph](http://www.brendangregg.com/flamegraphs.html)

```
sudo perf record -F 99 -a -g -- bash ./scripts/example_run.sh
perf script | stackcollapse-perf.pl > out.perf-folded
flamegraph.pl out.perf-folded > perf-kernel.svg
```