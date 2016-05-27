package fr.eurecom.dsg.cost

class Estimate(
  private var networkCost:Double = 0,
  private var cpuCost:Double = 0,
  private  var numInputRecords:Long = 0,
  private  var numOutputRecords:Long = 0,
  private var inputSize:Long = 0, // in bytes
  private  var outputSize:Long = 0){ // in bytes

  def getAvgInputTupleSize = inputSize*1.0/numInputRecords
  def getAvgOutputTupleSize = outputSize*1.0/numOutputRecords

  def addNumRecInput(amount:Long) { numInputRecords += amount }
  def getNumRecInput = numInputRecords
  def setNumRecInput(nRecs:Long) { numInputRecords = nRecs }

  def addNumRecOutput(amount:Long) { numOutputRecords += amount }
  def getNumRecOutput = numOutputRecords
  def setNumRecOutput(nRecs:Long){ numOutputRecords = nRecs }

  def addInputSize(nBytes:Long) { inputSize += nBytes }
  def getInputSize = inputSize
  def setInputSize(nBytes:Long) {inputSize = nBytes}

  def addOutputSize(nBytes:Long) { outputSize += nBytes }
  def getOutputSize = outputSize
  def setOutputSize(nBytes:Long) { outputSize = nBytes }

  def addNetworkCost(amount:Double) { networkCost += amount }
  def getNetworkCost:Double = networkCost

  def addCPUCost(cost:Double) { cpuCost += cost }
  def getCPUCost = cpuCost
  def setCPUCost(cost:Double) {cpuCost = cost}

  def getExecutionCost = cpuCost + networkCost


  override def toString():String={
    ("Estimation: cost:%.2f, Net: %.2f, Cpu: %.2f " +
      "\nnumInputRecords: %d, numOutputRecords: %d " +
      "\ninputSize: %d, outputSize: %d")
      .format(getExecutionCost, networkCost, cpuCost, numInputRecords, numOutputRecords, inputSize, outputSize)
  }

  def add(another:Estimate){
    this.networkCost += another.getNetworkCost
    this.cpuCost += another.getCPUCost
    this.numInputRecords += another.getNumRecInput
    this.numOutputRecords += another.getNumRecOutput
    this.inputSize += another.getInputSize
    this.outputSize += another.outputSize
  }
}
