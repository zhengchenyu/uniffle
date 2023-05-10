package org.apache.tez.runtime.library.common.shuffle.rss;

import java.util.concurrent.atomic.AtomicLong;

class PartitionInput { // 要读取的分区

  public static enum State {
    IDLE,               // No map outputs available
    BUSY,               // Map outputs are being fetched
    PENDING,            // Known map outputs which need to be fetched
    PENALIZED           // Host penalized due to shuffle failures
  }

  private int partitionId;
  private State state = State.IDLE;
  private final AtomicLong shuffleStart = new AtomicLong(0);      // TODO: 这个不是按照分区来的，可能不准确。


  PartitionInput(int id) {
    this.partitionId = id;
  }

  @Override
  public int hashCode() {
    return partitionId;
  }

  @Override
  public boolean equals(Object obj) {
    return this.partitionId == ((PartitionInput) obj).partitionId;
  }

  public synchronized void markBusy() {
    state = State.BUSY;
  }


  @Override
  public String toString() {
    return "PartitionInput{partitionId=" + partitionId + '}';
  }

  public int getPartitionId() {
    return partitionId;
  }

  public AtomicLong getShuffleStart() {
    return shuffleStart;
  }
}
