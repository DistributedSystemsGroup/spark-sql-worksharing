# Spark SQL Work Sharing Prototype

By exploiting the sharable common and similar subexpressions, Spark SQL Server transforms a batch of input queries into a new more efficient one which avoids unnecessary computations.

## Documentation
Please refer to our paper

## Applications in this project
### Pre-computing some basic statistics:
`ComputeStats <master> <inputDir> <savePath> <format>`
    + format = {parquet, csv}

### Micro-benchmark: Application to benchmark on simple queries

`fr.eurecom.dsg.microbenchmark.MicroBenchmark`

- arguments: <master> <inputFile> <format> <query> <mode>
    + master: {local, cluster}
    + inputFile
    + format: {parquet, csv}
    + query: {0, 1, 2, 3, 4}
    + mode: {wopt, opt} // witout optimization, with optimization
- Example: local /home/ntkhoa/random/random-csv csv 0 wopt

### Macro-benchmark:
`fr.eurecom.dsg.macrobenchmark.MacroBenchmark`

- arguments: <master> <inputFile> <format> <query> <mode>
    + master: {local, cluster}
    + inputFile
    + format: {parquet, csv}
    + query: {0, 1, 2, 3, 4}
    + mode: {wopt, opt} // witout optimization, with optimization
- Example: local /home/ntkhoa/random/random-csv csv 0 wopt

## Appendix
- Generating input data for the Micro-benchmark: microbenchmark-generator/generator.py

``python generator.py 1000000 | hdfs dfs -put - tmp/random-data.txt``

## Issues & resolution
- datetime datatype problem: change the data type to string

- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local: set `-XX:MaxPermSize=2G` (maximum size of Permanent Generation) in VM Options
    + in cluster: `--driver-java-options -XX:MaxPermSize=2G`