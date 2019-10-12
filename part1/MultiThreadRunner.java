package part1;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import part2.PerformanceStats;

public class MultiThreadRunner {
  private int numThreads;
  private int numSkiers;
  private int numLifts;
  private int numRuns;
  private Logger logger = LogManager.getLogger(PhaseThread.class.getName());
  private static AtomicInteger numSuccessfulRequest = new AtomicInteger(0);
  private static AtomicInteger numFailedRequest = new AtomicInteger(0);
  private static AtomicInteger numRequest = new AtomicInteger(0);
  private BlockingQueue<SharedData> records = new ArrayBlockingQueue<>(Constants.SHARED_DATA_RECORDS_CAPACITY);

  public MultiThreadRunner(int numThreads, int numSkiers, int numLifts, int numRuns) {
    this.numThreads = numThreads;
    this.numSkiers = numSkiers;
    this.numLifts = numLifts;
    this.numRuns = numRuns;
  }


  public void run(){
    // Construct CountDownLatch for controlling the starting time of different phases
    int numThreadsInFirstPhase = this.numThreads / 4;
    CountDownLatch currentLatchInFirstPhase = new CountDownLatch(1);
    CountDownLatch nextLatchInFirstPhase = new CountDownLatch((int) (0.1 * numThreadsInFirstPhase));

    int numThreadsInSecondPhase = this.numThreads;
    CountDownLatch currentLatchInSecondPhase = nextLatchInFirstPhase;
    CountDownLatch nextLatchInSecondPhase = new CountDownLatch((int) (0.1 * numThreadsInSecondPhase));

    CountDownLatch currentLatchInThirdPhase = nextLatchInSecondPhase;

    // Starting three phases
    long wallStart = System.currentTimeMillis();
    System.out.println("Start processing at " + new Date(wallStart).toString());
    Thread[] startUpThreads = startupPhase(currentLatchInFirstPhase, nextLatchInFirstPhase);
    Thread[] peakUpThreads = peakPhase(currentLatchInSecondPhase, nextLatchInSecondPhase);
    Thread[] coolDownThreads = coolDownPhase(currentLatchInThirdPhase, null);
    currentLatchInFirstPhase.countDown();
    joinThreads(startUpThreads);
    joinThreads(peakUpThreads);
    joinThreads(coolDownThreads);
    long wallEnd = System.currentTimeMillis();
    System.out.println("Stop processing at " + new Date(wallEnd).toString());

    // Print the performance stats data
    PerformanceStats performanceStats =
        new PerformanceStats(wallEnd - wallStart, this.records, Constants.numThreads,
            numSuccessfulRequest.intValue(), numFailedRequest.intValue(), numRequest.intValue());
    performanceStats.printStats();
  }

  private void joinThreads(Thread[] phaseThreads){
    for(Thread thread : phaseThreads){
      try{
        thread.join();
      } catch (InterruptedException e){
        logger.error(e.getMessage());
      }
    }
  }


  private Thread[] startupPhase(CountDownLatch currentLatch, CountDownLatch nextLatch) {
    Thread[] phaseThreads = new Thread[this.numThreads/4];
    int idRange = this.numSkiers / (this.numThreads / 4);
    int runTimes = (int) (this.numRuns * 0.1 * idRange);
    int startTime = 0;
    int endTime = 90;
    for (int i = 0; i < this.numThreads / 4; i++) {
      Thread thread = new Thread(new PhaseThread(
          idRange * i, idRange * i + idRange,
          startTime, endTime, runTimes, currentLatch, nextLatch, this.records, numSuccessfulRequest,
          numFailedRequest, numRequest));
      thread.start();
      phaseThreads[i] = thread;
    }
    return phaseThreads;
  }

  private Thread[] peakPhase(CountDownLatch currentLatch, CountDownLatch nextLatch) {
    Thread[] phaseThreads = new Thread[this.numThreads];
    int startTime = 91;
    int endTime = 360;
    int idRange = this.numSkiers / this.numThreads;
    int runTimes = (int) (numRuns * 0.8) * idRange;
    for (int i = 0; i < numThreads; i++) {
      Thread thread = new Thread(new PhaseThread(
          idRange * i, idRange * i + idRange,
          startTime, endTime, runTimes, currentLatch, nextLatch, records, numSuccessfulRequest,
          numFailedRequest, numRequest));
      thread.start();
      phaseThreads[i] = thread;
    }
    return phaseThreads;
  }

  private Thread[] coolDownPhase(CountDownLatch currentLatch, CountDownLatch nextLatch) {
    Thread[] phaseThreads = new Thread[this.numThreads/4];
    int startTime = 361;
    int endTime = 420;
    int idRange = this.numSkiers/ (this.numThreads / 4);
    int runTimes = (int) (this.numRuns * 0.1);

    for(int i = 0; i < this.numThreads/4; i++){
      Thread thread = new Thread(new PhaseThread(
          idRange * i, idRange * i + idRange, startTime,
          endTime, runTimes, currentLatch, nextLatch, this.records, numSuccessfulRequest,
          numFailedRequest, numRequest));
      thread.start();
      phaseThreads[i] = thread;
    }
    return phaseThreads;
  }
}
