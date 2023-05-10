package org.apache.tez.runtime.library.common.shuffle.rss;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progress;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.response.CompressedShuffleBlock;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class RssFetcherOrderedGrouped extends CallableWithNdc<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(RssFetcherOrderedGrouped.class);

  private static final AtomicInteger nextId = new AtomicInteger(0);


  // TODO: reporter for tez
//  private final Reporter reporter;

  private enum ShuffleErrors {
    CONNECTION, IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP, WRONG_REDUCE
  }

  private static final String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private static final double BYTES_PER_MILLIS_TO_MBS = 1000d / (ByteUnit.MiB.toBytes(1));
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  private final Configuration conf;

  private final InputContext context;

  // TODO: Counter
  private final TezCounter connectionErrs;
  private final TezCounter ioErrs;
  private final TezCounter wrongLengthErrs;
  private final TezCounter badIdErrs;
  private final TezCounter wrongMapErrs;
  private final TezCounter wrongReduceErrs;
  // private final TaskStatus status;
  private final MergeManager merger;
  private final Progress progress;
  // private final ShuffleClientMetrics metrics;
  private long totalBlockCount;
  private long copyBlockCount = 0;

  private volatile boolean stopped = false;

  private ShuffleReadClient shuffleReadClient;
  private long readTime = 0;
  private long decompressTime = 0;
  private long serializeTime = 0;
  private long waitTime = 0;
  private long copyTime = 0;  // the sum of readTime + decompressTime + serializeTime + waitTime
  private long unCompressionLength = 0;
  private long compressionLength = 0;

  // private final TezTaskAttemptID taskAttemptID;
  private int uniqueMapId = 0;

  private boolean hasPendingData = false;
  private long startWait;
  private int waitCount = 0;
  private byte[] uncompressedData = null;
  private RssConf rssConf;
  private CompressionCodec codec;

  private Codec rssCodec;


  private int uniqueAttemptNumber = 0;

  PartitionInput partitionInput;

  private final int id;

  private final int vertexId;
  private final int taskId;


  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  RssShuffleScheduler scheduler;

  RssFetcherOrderedGrouped(Configuration conf,
                           // TezTaskAttemptID taskAttemptID,
             InputContext context, PartitionInput partitionInput,
    /* TaskStatus status, */
             MergeManager merger, RssShuffleScheduler scheduler,
             Progress progress, CompressionCodec codec,
    /*Reporter reporter, ShuffleClientMetrics metrics,     // TODO: metrics改为TEZ的 */
             ShuffleReadClient shuffleReadClient,
             long totalBlockCount,
             RssConf rssConf, boolean ifileReadAhead, int ifileReadAheadLength) {
    this.conf = conf;
    // this.reporter = reporter;
    // this.status = status;
    this.merger = merger;
    this.scheduler = scheduler;
    this.vertexId = context.getTaskVertexIndex();
    this.taskId = context.getTaskIndex();
    this.id = nextId.incrementAndGet();
    this.progress = progress;
    // this.metrics = metrics;
    // this.taskAttemptID = taskAttemptID;
    this.context = context;
    this.partitionInput = partitionInput;
    this.connectionErrs = this.context.getCounters().findCounter(ShuffleErrors.CONNECTION);
    this.ioErrs = this.context.getCounters().findCounter(ShuffleErrors.IO_ERROR);
    this.wrongLengthErrs = this.context.getCounters().findCounter(ShuffleErrors.WRONG_LENGTH);
    this.badIdErrs = this.context.getCounters().findCounter(ShuffleErrors.BAD_ID);
    this.wrongMapErrs = this.context.getCounters().findCounter(ShuffleErrors.WRONG_MAP);
    this.wrongReduceErrs = this.context.getCounters().findCounter(ShuffleErrors.WRONG_REDUCE);

    this.shuffleReadClient = shuffleReadClient;
    this.totalBlockCount = totalBlockCount;

    this.rssConf = rssConf;
    this.codec = codec;
    this.rssCodec = Codec.newInstance(rssConf);
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
  }

  public void fetchAllRssBlocks() throws IOException, InterruptedException {
    while (!stopped) {
      try {
        // If merge is on, block
//        merger.waitForResource();
//        // Do shuffle
//        metrics.threadBusy();
        copyFromRssServer();
        scheduler.copySucceeded(null);      // TODO: add more information
      } finally {
        // TODO: 做一些统计信息
//        metrics.threadFree();
      }
    }
  }

  @VisibleForTesting
  public void copyFromRssServer() throws IOException {
    CompressedShuffleBlock compressedBlock = null;
    ByteBuffer compressedData = null;
    // fetch a block
    if (!hasPendingData) {
      final long startFetch = System.currentTimeMillis();
      compressedBlock = shuffleReadClient.readShuffleBlockData();
      if (compressedBlock != null) {
        compressedData = compressedBlock.getByteBuffer();
      }
      long fetchDuration = System.currentTimeMillis() - startFetch;
      readTime += fetchDuration;
    }

    // uncompress the block
    if (!hasPendingData && compressedData != null) {
      final long startDecompress = System.currentTimeMillis();
      int uncompressedLen = compressedBlock.getUncompressLength();
      ByteBuffer decompressedBuffer = ByteBuffer.allocate(uncompressedLen);
      rssCodec.decompress(compressedData, uncompressedLen, decompressedBuffer, 0);
      uncompressedData = decompressedBuffer.array();
      unCompressionLength += compressedBlock.getUncompressLength();
      compressionLength += compressedBlock.getByteBuffer().limit() - compressedBlock.getByteBuffer().position();
      long decompressDuration = System.currentTimeMillis() - startDecompress;
      decompressTime += decompressDuration;
    }

    if (uncompressedData != null) {
      // start to merge
      final long startSerialization = System.currentTimeMillis();
      if (issueMapOutputMerge()) {
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        serializeTime += serializationDuration;
        // if reserve successes, reset status for next fetch
        if (hasPendingData) {
          waitTime += System.currentTimeMillis() - startWait;
        }
        hasPendingData = false;
        uncompressedData = null;
      } else {
        // if reserve fail, return and wait
        startWait = System.currentTimeMillis();
        return;
      }

      // update some status
      copyBlockCount++;
      copyTime = readTime + decompressTime + serializeTime + waitTime;
      // updateStatus();
      context.notifyProgress();
    } else {
      // finish reading data, close related reader and check data consistent
      shuffleReadClient.close();
      shuffleReadClient.checkProcessedBlockIds();
      shuffleReadClient.logStatics();
      // metrics.inputBytes(unCompressionLength);
      LOG.info("Vertex-" +  vertexId + "  task " + taskId + " cost " + readTime + " ms to fetch and "
        + decompressTime + " ms to decompress with unCompressionLength["
        + unCompressionLength + "] and " + serializeTime + " ms to serialize and "
        + waitTime + " ms to wait resource");
      stopFetch();
    }
  }

  private boolean issueMapOutputMerge() throws IOException {
    InputAttemptIdentifier attemptIdentifier = getNextUniqueTaskAttemptID();
    MapOutput mapOutput = null;
    try {
      mapOutput = merger.reserve(attemptIdentifier, unCompressionLength, compressionLength, id);
      // mapOutput = merger.reserve(mapId, uncompressedData.length, 0);
    } catch (IOException ioe) {
      // kill this reduce attempt
      ioErrs.increment(1);
      throw ioe;
    }
    // Check if we can shuffle *now* ...
    if (mapOutput == null) {
      LOG.info("RssMRFetcher" + " - MergeManager returned status WAIT ...");
      // Not an error but wait to process data.
      // Use a retry flag to avoid re-fetch and re-uncompress.
      hasPendingData = true;
      waitCount++;
      return false;
    }

    // write data to mapOutput
    try {
      RssBypassWriter.write(mapOutput, uncompressedData);
      // let the merger knows this block is ready for merging
      mapOutput.commit();
      if (mapOutput instanceof MapOutput.DiskMapOutput) {
        LOG.info("Vertex-" + vertexId + " Task-" + taskId + " allocates disk to accept block "
          + " with byte sizes: " + uncompressedData.length);
      }
    } catch (Throwable t) {
      ioErrs.increment(1);
      mapOutput.abort();
      throw new RssException("Vertex-" + vertexId + " Task-" + taskId + " cannot write block to "
        + mapOutput.getClass().getSimpleName() + " due to: " + t.getClass().getName());
    }
    return true;
  }

  private InputAttemptIdentifier getNextUniqueTaskAttemptID() {
    // This function return a fake InputAttemptIdentifier. For Rss now inputIndex is partitionid, but not src task id.
    // And attemptNumber is increased_seq, but not task attempt id.
    InputAttemptIdentifier attemptIdentifier = new InputAttemptIdentifier(this.partitionInput.getPartitionId(), uniqueAttemptNumber++);
    return attemptIdentifier;
  }

  private void stopFetch() {
    stopped = true;
  }

//  private void updateStatus() {
//    progress.set((float) copyBlockCount / totalBlockCount);
//    String statusString = copyBlockCount + " / " + totalBlockCount + " copied.";
//    status.setStateString(statusString);
//
//    if (copyTime == 0) {
//      copyTime = 1;
//    }
//    double bytesPerMillis = (double) unCompressionLength / copyTime;
//    double transferRate = bytesPerMillis * BYTES_PER_MILLIS_TO_MBS;
//
//    progress.setStatus("copy(" + copyBlockCount + " of " + totalBlockCount + " at "
//      + mbpsFormat.format(transferRate) + " MB/s)");
//  }

  @VisibleForTesting
  public int getRetryCount() {
    return waitCount;
  }
  @Override
  protected Void callInternal() throws Exception {
    fetchAllRssBlocks();
    return null;
  }

  void shutDown() {

  }

}
