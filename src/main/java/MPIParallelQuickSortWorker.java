import static java.lang.Integer.toBinaryString;

import java.util.Arrays;
import mpi.Intracomm;
import mpi.MPI;

public class MPIParallelQuickSortWorker {

  private static final int ROOT_ID = 0;
  private int[] localValues;
  private int pivot;
  private IntArrayPair pair;

  public MPIParallelQuickSortWorker(int[] localPart) {
    this.localValues = localPart;
  }

  public int[] sort(Intracomm communicator) {
    if (communicator.Size() == 1) {
      Arrays.sort(localValues);
      return localValues;
    } else {
      findPivot(communicator);
      separateValues();
      exchangeAndUpdateLocalValues(communicator);
      return sort(getSubCommunicator(communicator));
    }
  }

  private void findPivot(Intracomm communicator) {
    int processRank = communicator.Rank();
    pivot = 0;

    if (processRank == ROOT_ID) {
      pivot = (localValues[0] + localValues[localValues.length - 1]) / 2;
    }

    sharePivotToOtherProcesses(communicator, pivot);
  }

  private void sharePivotToOtherProcesses(Intracomm communicator, int toSend) {
    int[] bufWithPivot = communicator.Rank() == ROOT_ID ? new int[]{toSend} : new int[1];

    communicator.Bcast(bufWithPivot, 0, 1, MPI.INT, ROOT_ID);

    pivot = bufWithPivot[0];
  }

  private void separateValues() {
    int[] less = new int[localValues.length];
    int[] greater = new int[localValues.length];

    int lessCounter = -1;
    int greaterCounter = -1;

    for (int value : localValues) {
      if (value > pivot) {
        greater[++greaterCounter] = value;
      } else {
        less[++lessCounter] = value;
      }
    }

    pair = new IntArrayPair(lessCounter, greaterCounter);
    System.arraycopy(less, 0 , pair.less, 0 , lessCounter + 1);
    System.arraycopy(greater, 0 , pair.greater, 0 , greaterCounter + 1);
  }

  private void exchangeAndUpdateLocalValues(Intracomm communicator) {
    if (isGreaterGroupProcess(communicator)) {
      updateLocalValues(pair.greater, sendAndReceivePart(communicator, pair.less));
    } else {
      updateLocalValues(pair.less, sendAndReceivePart(communicator, pair.greater));
    }
  }

  private boolean isGreaterGroupProcess(Intracomm communicator) {
    return getGreatestBitFromRank(communicator.Rank(), communicator.Size()) == 1;
  }

  private int[] sendAndReceivePart(Intracomm communicator, int[] toSend) {
    int pairProcessId = resolveProcessIdToExchange(communicator);
    int sizeToReceive = exchangeReceivingSize(communicator, toSend, pairProcessId);
    return exchangeValuesWithPair(communicator, toSend, pairProcessId, sizeToReceive);
  }

  private int exchangeReceivingSize(Intracomm communicator, int[] toSend, int remoteId) {
    ISend(communicator, new int[]{toSend.length}, remoteId);
    return Recv(communicator, 1, remoteId)[0];
  }

  private int[] exchangeValuesWithPair(Intracomm communicator, int[] toSend, int remoteId, int sizeToReceive) {
    ISend(communicator, toSend, remoteId);
    return Recv(communicator, sizeToReceive, remoteId);
  }

  private int resolveProcessIdToExchange(Intracomm communicator) {
    return swapGreatestBit(communicator.Rank(), communicator.Size());
  }

  private Intracomm getSubCommunicator(Intracomm communicator) {
    int key = communicator.Rank();
    int color = isGreaterGroupProcess(communicator) ? 1 : 0;

    return communicator.Split(color, key);
  }

  private void updateLocalValues(int[] myPart, int[] receivedPart) {
    int[] newLocalValues = new int[myPart.length + receivedPart.length];

    System.arraycopy(myPart, 0, newLocalValues, 0, myPart.length);
    System.arraycopy(receivedPart, 0, newLocalValues, myPart.length, receivedPart.length);

    localValues = newLocalValues;
  }

  public static int getGreatestBitFromRank(int rank, int clusterSize) {
    return Integer.parseInt(
        String.valueOf(
            String.format("%8s", toBinaryString(rank))
                .replace(' ', '0')
                .charAt(8 - toBinaryString(clusterSize - 1).length())));
  }

  public static int swapGreatestBit(int rank, int clusterSize) {
    return rank ^ (clusterSize >> 1);
  }

  private void ISend(Intracomm communicator, int[] toSend, int dest) {
    communicator.Isend(toSend, 0, toSend.length, MPI.INT, dest, 0);
  }

  private int[] Recv(Intracomm communicator, int size, int src) {
    int[] buff = new int[size];
    communicator.Recv(buff, 0, size, MPI.INT, src, 0);
    return buff;
  }

  public static class IntArrayPair {

    public int[] less;
    public int[] greater;

    public IntArrayPair(int lessCounter, int greaterCounter) {
      less = new int[lessCounter + 1];
      greater = new int[greaterCounter + 1];
    }
  }
}
