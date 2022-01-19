package org.momacmo.aws.lambda.jscio;

public class JscLambdaOutput {
  public int traceCount;
	public float iotime;
	public int iobytes;
	public JscLambdaInput input;
	public String status;
	
	public JscLambdaInput getInput() {
		return input;
	}
	public void setInput(JscLambdaInput input) {
		this.input = input;
	}
	public float getIotime() {
		return iotime;
	}
	public void setIotime(float iotime) {
		this.iotime = iotime;
	}
	public int getIobytes() {
		return iobytes;
	}
	public void setIobytes(int iobytes) {
		this.iobytes = iobytes;
	}
	public int getTraceCount() {
	  return traceCount;
	}
	public void setTraceCount( int traceCount ) {
	  this.traceCount = traceCount;
	}
	public String getStatus() {
	  return status;
	}
  public void setStatus(String status) {
    this.status = status;
  }
}
