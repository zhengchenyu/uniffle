package org.apache.tez.dag.common.rss;

import org.apache.hadoop.mapreduce.RssMRUtils;
import org.apache.uniffle.common.util.IdHelper;

public class TezIDHelper implements IdHelper {

  @Override
  public long getTaskAttemptId(long blockId, long shuffleId) {
    return RssTezUtils.getTaskAttemptId(blockId, shuffleId);
  }
}
