package org.apache.tez.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRssReportShuffleFetchFailureResponse {

  @Test
  public void testSerde() throws IOException {
    RssReportShuffleFetchFailureResponse response =
        new RssReportShuffleFetchFailureResponse(StatusCode.INTERNAL_ERROR, "error message");
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    DataOutput out = new DataOutputStream(baos);
    response.write(out);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput in = new DataInputStream(bais);
    RssReportShuffleFetchFailureResponse deResponse = new RssReportShuffleFetchFailureResponse();
    deResponse.readFields(in);
    Assertions.assertEquals(StatusCode.INTERNAL_ERROR, deResponse.getStatusCode());
    Assertions.assertEquals("error message", deResponse.getMessage());
  }
}
