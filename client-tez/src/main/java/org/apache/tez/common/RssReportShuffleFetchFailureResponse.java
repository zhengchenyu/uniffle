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
import org.apache.uniffle.common.rpc.StatusCode;

public class RssReportShuffleFetchFailureResponse implements Writable {

  private StatusCode statusCode;
  private String message;
  
  public RssReportShuffleFetchFailureResponse() {}

  public RssReportShuffleFetchFailureResponse(StatusCode code, String msg) {
    this.statusCode = code;
    this.message = msg;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeEnum(dataOutput, statusCode);
    WritableUtils.writeString(dataOutput, message);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.statusCode = WritableUtils.readEnum(dataInput, StatusCode.class);
    this.message = WritableUtils.readString(dataInput);
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public String getMessage() {
    return message;
  }
}
