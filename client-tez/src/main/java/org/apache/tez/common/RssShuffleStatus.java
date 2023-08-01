/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class RssShuffleStatus {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
  private final int[] partitions;
  private int vertexAttempt;

  public RssShuffleStatus(int partitionNum, int vertexAttempt) {
    this.vertexAttempt = vertexAttempt;
    this.partitions = new int[partitionNum];
  }

  private <T> T withReadLock(Supplier<T> fn) {
    readLock.lock();
    try {
      return fn.get();
    } finally {
      readLock.unlock();
    }
  }

  private <T> T withWriteLock(Supplier<T> fn) {
    writeLock.lock();
    try {
      return fn.get();
    } finally {
      writeLock.unlock();
    }
  }

  // todo: maybe it's more performant to just use synchronized method here.
  public int getVertexAttempt() {
    return withReadLock(() -> this.vertexAttempt);
  }

  /**
   * Check whether the input vertex attempt is a new vertx or not. If a new vertex attempt is
   * requested, reset partitions.
   *
   * @param vertexAttempt the incoming vertex attempt number
   * @return 0 if vertexAttempt == this.vertexAttempt 1 if vertexAttempt > this.vertexAttempt -1 if
   *     stateAttempt < this.vertexAttempt which means nothing happens
   */
  public int resetVertexAttemptIfNecessary(int vertexAttempt) {
    return withWriteLock(
        () -> {
          if (this.vertexAttempt < vertexAttempt) {
            // a new vertex attempt is issued. the partitions array should be clear and reset.
            Arrays.fill(this.partitions, 0);
            this.vertexAttempt = vertexAttempt;
            return 1;
          } else if (this.vertexAttempt > vertexAttempt) {
            return -1;
          }
          return 0;
        });
  }

  public void incPartitionFetchFailure(int vertexAttempt, int partition) {
    withWriteLock(
        () -> {
          if (this.vertexAttempt != vertexAttempt) {
            // do nothing here
          } else {
            this.partitions[partition] = this.partitions[partition] + 1;
          }
          return null;
        });
  }

  public int getPartitionFetchFailureNum(int vertexAttempt, int partition) {
    return withReadLock(
        () -> {
          if (this.vertexAttempt != vertexAttempt) {
            return 0;
          } else {
            return this.partitions[partition];
          }
        });
  }

  public int getTotalFetchFailureNum(int vertexAttempt) {
    return withReadLock(() -> {
     if (this.vertexAttempt != vertexAttempt) {
       return 0;
     } else {
       return Arrays.stream(this.partitions).sum();
     }
    });
  }
}
