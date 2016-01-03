package nw.fr.eurecom.dsg.util

import org.apache.spark.sql.myExtensions.optimizer.CacheAwareOptimizer
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by ntkhoa on 09/12/15.
  */
object QueryExecutor{

  def execute(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
    executeSequential(sqlContext, dfs, outputPath)
    executeWorkSharing(sqlContext, dfs, outputPath)
  }

  def executeSequential(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    dfs.zipWithIndex.foreach{case(df,i) => df.write.save(outputPath + i.toString)}
  }

  def executeWorkSharing(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans, sqlContext)
    //val bestStrategy = strategyGenerator.next()

        //var bestStrategy = strategyGenerator.next()
        while(strategyGenerator.hasNext()){
          val bestStrategy = strategyGenerator.next()
          println("new strategy is generated")
          println(bestStrategy)
          val df = bestStrategy.execute(sqlContext)

         //comparing the cost
        }
        //bestStrategy

//    val df = bestStrategy.execute(sqlContext)
//    df.indices.foreach(i => df(i).write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save(outputPath + "/" + i.toString))
  }

  def executeWorkSharing(strategyIndex:Int, sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans, sqlContext)
    var i = -1

    while(strategyGenerator.hasNext()){
      val bestStrategy = strategyGenerator.next()
      i+=1
      if(i == strategyIndex){
        val df = bestStrategy.execute(sqlContext)
            df.indices.foreach(i => df(i).write.format("com.databricks.spark.csv")
              .option("header", "true")
              .save(outputPath + "/" + i.toString))
      }
    }

  }

}
