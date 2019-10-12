package part1;

public class SharedData {
  private long startTime;
  private long latency;
  private int responseCode;
  private String requestMethod;

  public SharedData(long startTime, long latency, int responseCode) {
    this.startTime = startTime;
    this.latency = latency;
    this.responseCode = responseCode;
    this.requestMethod = "POST";
  }

  public long getStartTime() {
    return startTime;
  }

  public long getLatency() {
    return latency;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public String getRequestMethod() {
    return requestMethod;
  }
}
