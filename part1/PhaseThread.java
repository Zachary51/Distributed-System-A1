package part1;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PhaseThread extends Thread implements Runnable{
  private int startSkierId;
  private int endSkierId;
  private int startTime;
  private int endTime;
  private int runTimes;
  private AtomicInteger numSuccessRequest;
  private AtomicInteger numFailedRequest;
  private AtomicInteger numRequest;
  private CountDownLatch currentCountdownLatch;
  private CountDownLatch nextPhaseCountdownLatch;
  private BlockingQueue<SharedData> sharedRecords;
  public static final Logger logger = LogManager.getLogger(PhaseThread.class.getName());


  public PhaseThread(int startSkierId, int endSkierId,
      int startTime, int endTime, int runTimes, CountDownLatch currentLatch, CountDownLatch nextLatch,
      BlockingQueue<SharedData> sharedRecords, AtomicInteger numSuccessRequest, AtomicInteger numFailedRequest,
      AtomicInteger numRequest){
    this.startSkierId = startSkierId;
    this.endSkierId = endSkierId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.runTimes = runTimes;
    this.currentCountdownLatch = currentLatch;
    this.nextPhaseCountdownLatch = nextLatch;
    this.sharedRecords = sharedRecords;
    this.numSuccessRequest = numSuccessRequest;
    this.numFailedRequest = numFailedRequest;
    this.numRequest = numRequest;
  }

  @Override
  public void run() {
    try{
      currentCountdownLatch.await();  // Wait until part of threads in the previous phase finish their jobs
      for(int i = 0; i < runTimes; i++){
        try {
          String targetUrl =  Constants.LOCAL_ENV ? Constants.LOCAL_URL : Constants.REMOTE_URL;
          SkiersApi apiInstance = new SkiersApi();
          ApiClient client = apiInstance.getApiClient();
          client.setBasePath(targetUrl);

          long start = System.currentTimeMillis();
          ApiResponse<Integer> response = apiInstance.getSkierDayVerticalWithHttpInfo(1, "1",
              "1", ThreadLocalRandom.current().nextInt(startSkierId, endSkierId) + 1);
          this.numRequest.getAndIncrement();
          long latency = System.currentTimeMillis() - start;
          if(response.getStatusCode() == 200 || response.getStatusCode() == 201){
            this.numSuccessRequest.getAndIncrement();
          } else {
            this.numFailedRequest.getAndIncrement();
          }
          this.sharedRecords.add(new SharedData(start, latency, response.getStatusCode()));
        } catch (ApiException e){
          logger.error(e.getMessage());
        }
      }
    } catch (InterruptedException e){
      logger.info(e.getMessage());
    } finally {
      if(this.nextPhaseCountdownLatch != null){
        this.nextPhaseCountdownLatch.countDown();
      }
    }
  }

}
