package nw.fr.eurecom.dsg.cost

class Estimate(
  private var networkCost:Double = 0,
  private var cpuCost:Double = 0,
  private  var numRecInput:Long = 0,
  private  var numRecOutput:Long = 0,
  private var inputSize:Long = 0,
  private  var outputSize:Long = 0){

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
    "Estimate: Net: %.2f, Cpu: %.2f \nNumRInput: %d, NumROutput: %d \nInputSize: %d, OutputSize: %d"
      .format(networkCost, cpuCost, numRecInput, numRecOutput, inputSize, outputSize)
  }

  def add(another:Estimate):Estimate={
    new Estimate(this.networkCost + another.getNetworkCost,
      this.cpuCost + another.getCPUCost,
      this.numRecInput + another.getNumRecInput,
      this.numRecOutput + another.getNumRecOutput,
      this.inputSize + another.getInputSize,
      this.outputSize + another.outputSize
    )
  }
}
