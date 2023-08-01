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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RssReportShuffleFetchFailureRequest implements Writable {

  private String appId;
  private int shuffleId;
  private int vertexAttemptId;
  private int partitionId;
  private String exception;

  RssReportShuffleFetchFailureRequest() {}

  public RssReportShuffleFetchFailureRequest(String appId, int shuffleId, int vertexAttemptId, int partitionId,
                                             String exception) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.vertexAttemptId = vertexAttemptId;
    this.partitionId = partitionId;
    this.exception = exception;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, appId);
    WritableUtils.writeVInt(dataOutput, shuffleId);
    WritableUtils.writeVInt(dataOutput, vertexAttemptId);
    WritableUtils.writeVInt(dataOutput, partitionId);
    WritableUtils.writeString(dataOutput, exception);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.appId = WritableUtils.readString(dataInput);
    this.shuffleId = WritableUtils.readVInt(dataInput);
    this.vertexAttemptId = WritableUtils.readVInt(dataInput);
    this.partitionId = WritableUtils.readVInt(dataInput);
    this.exception = WritableUtils.readString(dataInput);
  }

  @Override
  public String toString() {
    return "RssReportShuffleFetchFailureRequest{" +
        "appId='" + appId + '\'' +
        ", shuffleId=" + shuffleId +
        ", vertexAttemptId=" + vertexAttemptId +
        ", partitionId=" + partitionId +
        ", exception='" + exception + '\'' +
        '}';
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getVertexAttemptId() {
    return vertexAttemptId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getException() {
    return exception;
  }
}
