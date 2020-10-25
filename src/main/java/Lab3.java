import generator.InputGenerator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import mpi.MPI;

public class Lab3 {

  public static final int ROOT_ID = 0;
  public static int inputSize = 0;
  public static String inputFileName = "";
  public static String outputFileName = "";
  public static int[] result;

  public static void main(String[] args) throws IOException {
    switch (Integer.parseInt(args[3])) {
      case 1:
        System.out.println("Run program...");
        inputFileName = args[4];
        outputFileName = args[5];

        MPI.Init(args);

        MPIParallelQuickSortWorker parallelQuickSort = new MPIParallelQuickSortWorker(
            splitIntoParts(MPI.COMM_WORLD.Size()));

        System.gc();
        MPI.COMM_WORLD.Barrier();

        System.out.println("Start Calculation...");
        long executionTime = getExecutionTime(parallelQuickSort);
        System.out.println("End Calculation...");

        if (MPI.COMM_WORLD.Rank() == ROOT_ID) {
          saveResult();
          System.out.printf("Execution time - %s", executionTime);
        }

        MPI.Finalize();
        break;
      case 2:
        System.out.println("Generate input...");
        String outputPath = args[4];
        int size = Integer.parseInt(args[5]);
        int maxValue = Integer.parseInt(args[6]);
        InputGenerator.generate(outputPath, size, maxValue);
        System.out.println("Done...");
        break;
      default:
    }
  }

  private static long getExecutionTime(MPIParallelQuickSortWorker parallelQuickSort)
      throws IOException {
    long start = System.currentTimeMillis();
    int[] localResult = parallelQuickSort.sort(MPI.COMM_WORLD);
    collectResult(localResult);
    long end = System.currentTimeMillis();
    return end - start;
  }

  static int[] splitIntoParts(int numberOfProcesses) throws IOException {
    int partSize = 0;
    int[] allValues = null;
    if (MPI.COMM_WORLD.Rank() == ROOT_ID) {
      System.out.println("Loading input data...");
      allValues = loadInput();
      System.out.println("End loading...");
      inputSize = allValues.length;
      partSize = inputSize / numberOfProcesses;

      if (numberOfProcesses == 1) {
        return allValues;
      }
    }

    int[] bufWithPartSize = new int[]{partSize};
    MPI.COMM_WORLD.Bcast(bufWithPartSize, 0, 1, MPI.INT, ROOT_ID);
    partSize = bufWithPartSize[0];

    int[] localPart = new int[partSize];

    if (MPI.COMM_WORLD.Rank() == ROOT_ID) {
      MPI.COMM_WORLD.Scatter(allValues, 0, partSize, MPI.INT,
          localPart, 0, partSize, MPI.INT, ROOT_ID);

      int remainder = allValues.length % numberOfProcesses;
      int[] rootPart = new int[partSize + remainder];
      int[] remainderPart = Arrays.copyOfRange(allValues, partSize * MPI.COMM_WORLD.Size(),
          partSize * MPI.COMM_WORLD.Size() + remainder);

      System.arraycopy(localPart, 0, rootPart, 0, partSize);
      System.arraycopy(remainderPart, 0, rootPart, localPart.length, remainderPart.length);

      return rootPart;
    } else {
      allValues = new int[partSize * MPI.COMM_WORLD.Size()];
      MPI.COMM_WORLD.Scatter(allValues, 0, partSize, MPI.INT,
          localPart, 0, partSize, MPI.INT, ROOT_ID);
    }

    return localPart;
  }

  private static void collectResult(int[] myNewSortedPart) throws IOException {
    int[] ints = {myNewSortedPart.length};
    int[] resultSizes = new int[MPI.COMM_WORLD.Size()];

    MPI.COMM_WORLD.Gather(ints, 0, 1, MPI.INT,
        resultSizes, 0, 1, MPI.INT, ROOT_ID);

    if (MPI.COMM_WORLD.Rank() == ROOT_ID) {
      result = new int[inputSize];
      int[] offsets = new int[MPI.COMM_WORLD.Size()];
      offsets[0] = 0;

      for (int i = 1; i < MPI.COMM_WORLD.Size(); i++) {
        offsets[i] = offsets[i - 1] + resultSizes[i - 1];
      }

      MPI.COMM_WORLD.Gatherv(myNewSortedPart, 0, myNewSortedPart.length, MPI.INT,
          result, 0, resultSizes, offsets, MPI.INT, ROOT_ID);
    } else {
      MPI.COMM_WORLD.Gatherv(myNewSortedPart, 0, myNewSortedPart.length, MPI.INT,
          null, 0, null, null, MPI.INT, ROOT_ID);
    }
  }

  static int[] loadInput() throws IOException {
    try (BufferedReader inputReader = new BufferedReader(new FileReader(inputFileName))) {
      return Arrays.stream(inputReader.readLine().split(" ")).mapToInt(Integer::parseInt).toArray();
    }
  }

  private static void saveResult() throws IOException {
    try (BufferedWriter inputWriter = new BufferedWriter(new FileWriter(outputFileName))) {
      inputWriter.write(Arrays.toString(result).replaceAll("[\\[\\],]", " "));
    }
  }
}
