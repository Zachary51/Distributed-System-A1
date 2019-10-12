package part1;

import java.util.Scanner;

public class Processor {
  public static void main(String[] args) throws Exception{
    getArguments();

    MultiThreadRunner runner = new MultiThreadRunner(
        Constants.numThreads, Constants.numSkiers, Constants.numLifts, Constants.numRuns);
    runner.run();
  }

  private static void getArguments(){
    Scanner in = new Scanner(System.in);
    String line;
    // Get the number of threads from user's input
    System.out.println("Please enter the number of threads. (The max number of threads is "
        + Constants.MAX_NUM_THREADS + ")");
    System.out.println("The default number of threads is " + Constants.DEFAULT_NUM_THREADS);
    line = in.nextLine();
    if (!line.equals("")) {
      Constants.numThreads = Integer.parseInt(line);
    } else {
      Constants.numThreads = Constants.DEFAULT_NUM_THREADS;
    }

    // Get the number of skiers from user's input
    System.out.println("Please enter the number of skiers. (The max number of skiers is "
        + Constants.MAX_NUM_SKIERS + ")");
    line = in.nextLine();
    if(!line.equals("")){
      Constants.numSkiers = Integer.parseInt(line);
    }

    // Get the number of lifts from user's input
    System.out.println(String.format("Please enter the number of lifts. "
        + "(The range of lifts should be in %s and %s)", Constants.MIN_SKI_LIFTS, Constants.MAX_SKI_LIFTS));
    System.out.println("The default number of lifts is " + Constants.DEFAULT_SKI_LIFTS);
    line = in.nextLine();
    if (line.equals("")) {
      Constants.numLifts = Constants.DEFAULT_SKI_LIFTS;
    } else {
      Constants.numLifts = Integer.parseInt(line);
    }

    // Get the run times from user's input
    System.out.println("Please enter the mean number of lifts each skier rides each day.");
    System.out.println("The max number is " + Constants.MAX_NUM_RUN);
    System.out.println("The default number is " + Constants.DEFAULT_NUM_RUN);
    line = in.nextLine();
    if (line.equals("")) {
      Constants.numRuns = Constants.DEFAULT_NUM_RUN;
    } else {
      Constants.numRuns = Integer.parseInt(line);
    }
    argumentsValidator(Constants.numThreads, Constants.numSkiers, Constants.numLifts, Constants.numRuns);
  }

  private static void argumentsValidator(int numThread, int numSkiers, int numLifts, int numRun){
    if(numThread < 0 || numThread > Constants.MAX_NUM_THREADS){
      throw new IllegalArgumentException("Number of threads is out of range");
    }
    if(numSkiers < 0 || numSkiers >Constants. MAX_NUM_SKIERS){
      throw new IllegalArgumentException("Number of skiers is out of range");
    }
    if(numLifts < Constants.MIN_SKI_LIFTS || numLifts > Constants.MAX_SKI_LIFTS){
      throw new IllegalArgumentException("Number of ski lifts is out of range");
    }
    if(numRun < 0 || numRun > Constants.MAX_NUM_RUN){
      throw new IllegalArgumentException("Mean number of ski lifts is out of range");
    }
  }
}
