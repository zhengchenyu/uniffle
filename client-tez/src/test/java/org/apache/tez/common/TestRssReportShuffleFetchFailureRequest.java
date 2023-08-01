package org.apache.tez.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRssReportShuffleFetchFailureRequest {
  
  @Test
  public void testSerde() throws IOException {
    RssReportShuffleFetchFailureRequest request =
        new RssReportShuffleFetchFailureRequest("appId", 100, 101, 102, "test exception");
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    DataOutput out = new DataOutputStream(baos);
    request.write(out);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput in = new DataInputStream(bais);
    RssReportShuffleFetchFailureRequest deRequest = new RssReportShuffleFetchFailureRequest();
    deRequest.readFields(in);
    Assertions.assertEquals("appId", deRequest.getAppId());
    Assertions.assertEquals(100, deRequest.getShuffleId());
    Assertions.assertEquals(101, deRequest.getVertexAttemptId());
    Assertions.assertEquals(102, deRequest.getPartitionId());
    Assertions.assertEquals("test exception", deRequest.getException());
  }
}
