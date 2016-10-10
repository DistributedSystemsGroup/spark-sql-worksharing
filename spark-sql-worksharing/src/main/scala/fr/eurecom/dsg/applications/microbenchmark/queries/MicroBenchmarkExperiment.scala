package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.DataFrame

/**
  * Abstract class for MicroBenchmarkExperiment
  * A MicroBenchmarkExperiment consists of 2 queries
  * Two strategies to run the 2 queries: runWithoutWorkSharing and runWithWorkSharing
  *
  * @param data : input DataFrame
  */
abstract class MicroBenchmarkExperiment(data: DataFrame) {

  /**
    * get the DataFrame of query 1 (without Work Sharing)
    */
  def query1: DataFrame

  /**
    * get the DataFrame of query 2 (without Work Sharing)
    */
  def query2: DataFrame

  /**
    * get the DataFrame of cache plan (with Work Sharing)
    */
  def cachePlan: DataFrame

  /**
    * get the DataFrame of query 1 (with Work Sharing)
    */
  def query1WS: DataFrame

  /**
    * get the DataFrame of query 2 (with Work Sharing)
    */
  def query2WS: DataFrame

  /**
    * Invoke the ``warm up" process: scanning the input relation once
    * This will trigger the disk caching of the OS
    */
  def warmUp(): Unit = {
    data.foreach(_ => ())
  }

  /**
    * We run 2 queries in order after invoke the ``warm up" process because we want to make a fair comparison about the execution time between 2 queries
    */
  def runWithoutWorkSharing(): Unit = {
    warmUp()
    query1.foreach(_ => ())
    query2.foreach(_ => ())
  }

  /**
    * We run 2 queries in order after invoke the ``warm up" process because we want to make a fair comparison about the execution time between 2 queries
    * ``cachePlan.cache()" statement will just notify the CacheManager that some data needs to be cached
    */
  def runWithWorkSharing(): Unit = {
    warmUp()
    cachePlan.cache()
    query1WS.foreach(_ => ())
    query2WS.foreach(_ => ())
  }
}
