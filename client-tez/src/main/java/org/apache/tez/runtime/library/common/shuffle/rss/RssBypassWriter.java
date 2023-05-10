package org.apache.tez.runtime.library.common.shuffle.rss;

import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.InMemoryMapOutput;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.DiskMapOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssBypassWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RssBypassWriter.class);

  public static void write(MapOutput mapOutput, byte[] buffer) {
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput instanceof InMemoryMapOutput) {
      InMemoryMapOutput inMemoryMapOutput = (InMemoryMapOutput) mapOutput;
      write(inMemoryMapOutput, buffer);
    } else if (mapOutput instanceof DiskMapOutput) {
      // RSS leverages its own compression, it is incompatible with hadoop's disk file compression.
      // So we should disable this situation.
      // TODO: 这里需要支持磁盘合并，否则无法处理大规模数据。
      throw new IllegalStateException("RSS does not support OnDiskMapOutput as shuffle ouput,"
        + " try to reduce mapreduce.reduce.shuffle.memory.limit.percent");
    } else {
      throw new IllegalStateException("Merger reserve unknown type of MapOutput: "
        + mapOutput.getClass().getCanonicalName());
    }
  }

  private static void write(InMemoryMapOutput inMemoryMapOutput, byte[] buffer) {
    byte[] memory = inMemoryMapOutput.getMemory();
    System.arraycopy(buffer, 0, memory, 0, buffer.length);
  }
}
