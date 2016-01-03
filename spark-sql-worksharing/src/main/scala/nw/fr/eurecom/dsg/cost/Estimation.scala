package nw.fr.eurecom.dsg.cost

class Estimation(
  private var networkCost:Double = CostConstants.UNKNOWN_COST,
  private var cpuCost:Double = CostConstants.UNKNOWN_COST,
  private  var numRecInput:Long = CostConstants.UNKNOWN,
  private  var numRecOutput:Long = CostConstants.UNKNOWN,
  private var inputSize:Long = CostConstants.UNKNOWN,
  private  var outputSize:Long = CostConstants.UNKNOWN){

  def getAvgInputTupleSize():Double = {
    inputSize*1.0/numRecInput
  }

  def getAvgOutputTupleSize():Double = {
    outputSize*1.0/numRecOutput
  }

  def addnumRecInput(nRecs:Long)= numRecInput += nRecs
  def getNumRecInput():Long=numRecInput

  def addnumRecOutput(nRecs:Long)= numRecOutput += nRecs
  def getNumRecOutput():Long=numRecOutput
  def setNumRecOutput(nRecs:Long) = numRecOutput = nRecs

  def addInputSize(nBytes:Long)= inputSize += nBytes
  def getInputSize:Long = inputSize

  def addOutputSize(nBytes:Long)= outputSize += nBytes
  def getOutputSize:Long = outputSize
  def setOutputSize(nBytes:Long) = outputSize = nBytes

  def addNetworkCost(cost:Double)= networkCost += cost
  def getNetworkCost:Double = networkCost

  def addCPUCost(cost:Double)=  cpuCost += cost
  def getCPUCost:Double = cpuCost

  def getExecutionCost()= cpuCost + networkCost


  override def toString():String={
    "Net: %.2f, Cpu: %.2f \nNumRInput: %d, NumROutput: %d \nInputSize: %d, OutputSize: %d"
      .format(networkCost, cpuCost, numRecInput, numRecOutput, inputSize, outputSize)
  }
}
