# Spark SQL Work Sharing Prototype

By exploiting the sharable common and similar subexpressions, Spark SQL Server transforms a batch of input queries into a new more efficient one which avoids unnecessary computations.

## Documentation
Please refer to our paper

## Applications in this project
### Pre-computing some basic statistics:
`fr.eurecom.dsg.ComputeStats`

- Arguments: <master> <inputDir> <savePath> <format>
    + master = {local, cluster}
    + inputDir: where to read the input data
    + savePath: where to save the statistics (output)
    + format = {parquet, csv}

### Micro-benchmark: Application to benchmark on simple queries

`fr.eurecom.dsg.microbenchmark.MicroBenchmark`

- Arguments: <master> <inputFile> <format> <experimentNum> <mode>
    + master: {local, cluster}
    + inputFile
    + format: {parquet, csv}
    + experimentNum: {0, 1, 2, 3, 4, 100, 101, 102}
    + mode: {wopt, opt} // without Work Sharing, with Work Sharing
- Example: local /home/ntkhoa/random/random-csv csv 0 wopt

### Macro-benchmark:
`fr.eurecom.dsg.macrobenchmark.MacroBenchmark`

- Arguments: <master> <inputDir> <outputDir> <format> <statFile> <mode> <nQueries> <randomlySelect> <seeds>
    + master: {local, cluster}
    + inputDir
    + outputDir
    + format: {parquet, csv}
    + statFile
    + mode: {wopt, opt} // without Work Sharing, with Work Sharing
    + nQueries // how many queries you want to run in an experiment
    + randomlySelect: {True, False}
    + seeds // random seeds

## Appendix
- Generating input data for the Micro-benchmark: microbenchmark-generator/generator.py

`python generator.py 1000000 | hdfs dfs -put - tmp/random-data.txt`

To convert txt format to csv and parquet, use fr.eurecom.dsg.microbenchmark.ConvertData

## Issues & resolution
- datetime datatype problem: change the data type to string

- Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main": give Spark more memory to initialize HiveContext.
    + in local: set `-XX:MaxPermSize=2G` (maximum size of Permanent Generation) in VM Options
    + in cluster: `--driver-java-options -XX:MaxPermSize=2G`