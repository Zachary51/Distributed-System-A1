package part2;

import static part1.PhaseThread.logger;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import part1.Constants;
import part1.SharedData;

public class PerformanceStats {
  private long wallTime;
  private HashMap<String, Long> latencyStats = new HashMap<>();
  private BlockingQueue<SharedData> sharedData;
  private List<Long> latency;
  private long latencySum;
  private int numThreads;
  private int numSuccessRequests;
  private int numFailedRequests;
  private int numRequests;

  public PerformanceStats(long wallTime, BlockingQueue<SharedData> sharedData, int numThreads,
      int numSuccessRequests, int numFailedRequests, int numRequests){
    this.wallTime = wallTime;
    this.sharedData = sharedData;
    this.latencySum = 0;
    this.latency = new ArrayList<>();
    this.numThreads = numThreads;
    this.numSuccessRequests = numSuccessRequests;
    this.numFailedRequests = numFailedRequests;
    this.numRequests = numRequests;
  }

  public void printStats(){
    generateCsvFile();
    System.out.println("==========================Begin of Information=============================");
    System.out.println("Performance statistics: " + Constants.numThreads + " threads");
    System.out.println("---------------------------------------------------------------------------");
    System.out.println("The total number of requests: " + this.numRequests);
    System.out.println("The total number of successful requests: " + this.numSuccessRequests);
    System.out.println("The total number of failed requests: " + this.numFailedRequests);
    System.out.println("The wall time: " + wallTime/1000 + " s");
    calculate();
    System.out.println("---------------------------------------------------------------------------");
    System.out.println("Mean value of latencies: " + latencyStats.get("mean"));
    System.out.println("Median of latencies: " + latencyStats.get("median"));
    System.out.println("Throughput: " + latencyStats.get("throughput"));
    System.out.println("99th percentile of latencies: " + latencyStats.get("p99"));
    System.out.println("Max response time: " + latencyStats.get("max"));
    System.out.println("==========================End of Information===============================");
  }

  private void calculate(){
    Collections.sort(latency);
    this.latencyStats.put("mean", latencySum / latency.size()); // Mean of latencies
    this.latencyStats.put("median", latency.get(latency.size() / 2 -1)); // Median
    this.latencyStats.put("throughput", (this.numSuccessRequests + this.numFailedRequests) / wallTime); // Throughput
    this.latencyStats.put("p99", latency.get(latency.size() * 99 / 100 - 1)); // 99th percentile
    this.latencyStats.put("max", latency.get(latency.size() - 1)); // Max response time

  }

  private void generateCsvFile(){
    try{
      BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(generateFileName()), "UTF-8"));
      for(SharedData data: this.sharedData){
        long currentLatency = data.getLatency();
        latencySum += currentLatency;
        latency.add(currentLatency);
        StringBuilder builder = new StringBuilder();
        builder.append(data.getStartTime());
        builder.append(",");
        builder.append(data.getRequestMethod());
        builder.append(",");
        builder.append(data.getLatency());
        builder.append(",");
        builder.append(data.getResponseCode());
        bufferedWriter.write(builder.toString());
        bufferedWriter.newLine();
      }
      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (IOException e){
      logger.error(e.getMessage());
    }
  }

  private String generateFileName(){
    String fileName = this.numThreads + "-threads.csv";
    return Constants.LOCAL_CSV_DIR + fileName;
  }
}
