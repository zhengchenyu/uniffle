package org.apache.tez.runtime.library.common.writers.rss;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.SortWriteBuffer;
import org.apache.hadoop.mapreduce.RssMRConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.compression.Codec;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ByteUnit;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.storage.util.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB;
import static org.apache.tez.runtime.library.api.TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB_DEFAULT;

public class WriteBufferManager<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);

  private OutputContext outputContext;
  private Configuration conf;
  private RssConf rssConf;
  private boolean sort;
  private int numPartitions;

  private final Map<Integer, SortWriteBuffer<K, V>> buffers = JavaUtils.newConcurrentMap();
  private final List<SortWriteBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  private final ReentrantLock memoryLock = new ReentrantLock();
  private final Condition full = memoryLock.newCondition();

  private final AtomicLong memoryUsedSize = new AtomicLong(0);
  private final AtomicLong inSendListBytes = new AtomicLong(0);

  private final long maxMemSize;
  final RawComparator comparator;

  private final int batch;

  final long maxSegmentSize;
  final int sendThreadNum;
  final long maxBufferSize;
  final double memoryThreshold;
  final double sendThreshold;
  final long sendCheckInterval;
  final long sendCheckTimeout;
  final int bitmapSplitNum;
  final String appId;
  final int shuffleId;

  protected final Class keyClass;
  protected final Class valClass;
  protected final SerializationFactory serializationFactory;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  private final Codec codec;

  private final long taskAttemptId;

  private final Set<Long> allBlockIds = Sets.newConcurrentHashSet();

  private final Map<Integer, List<Long>> partitionToBlocks = JavaUtils.newConcurrentMap();

  private final ExecutorService sendExecutorService;

  private final ShuffleWriteClient shuffleWriteClient;

  private final boolean isMemoryShuffleEnabled;

  private Set<Long> successBlockIds = Sets.newConcurrentHashSet();
  private Set<Long> failedBlockIds = Sets.newConcurrentHashSet();


  // Counter
  private long copyTime = 0;
  private long sortTime = 0;
  private long compressTime = 0;
  private long uncompressedDataLen = 0;

  private int numTasks;

  private long[] sizePerPartition;

  public WriteBufferManager(OutputContext outputContext, Configuration conf, boolean sort, int numPartitions,
      long taskAttemptId, Map<Integer, List<ShuffleServerInfo>> partitionToServers) throws IOException {
    this.outputContext = outputContext;
    // conf include extraConf from RssDagAppMaster
    this.conf = conf;
    this.rssConf = RssTezUtils.toRssConf(this.conf);
    this.sort = sort;
    this.numPartitions = numPartitions;
    this.partitionToServers = partitionToServers;

    int sortmb = this.conf.getInt(TEZ_RUNTIME_IO_SORT_MB, TEZ_RUNTIME_IO_SORT_MB_DEFAULT);
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
        "Invalid \"" + TEZ_RUNTIME_IO_SORT_MB + "\": " + sortmb);
    }
    this.maxMemSize = (long) ByteUnit.MiB.toBytes(sortmb);     // TODO: 是否需要增加系数

    //TezOutputContextImpl tezTaskContext = (TezOutputContextImpl) this.outputContext;
    this.appId = RssTezUtils.constructAppId(outputContext.getApplicationId(), outputContext.getDAGAttemptNumber());
    this.shuffleId = this.conf.getInt(RssTezConfig.RSS_ASSIGNMENT_SHUFFLE_ID, -1);
    //assert vertexId != -1;
    if (shuffleId == -1) {
      throw new IOException("Config " + RssTezConfig.RSS_ASSIGNMENT_SHUFFLE_ID + " in WriteBufferManager is not set!");
    }
    this.numTasks = outputContext.getVertexParallelism();       // TODO: check it!!! 检查这个是实际运行task的并行度，还是写分区的个数?????

    this.comparator = ConfigUtils.getIntermediateOutputKeyComparator(this.conf);

    this.maxSegmentSize = conf.getLong(RssTezConfig.RSS_CLIENT_MAX_SEGMENT_SIZE,
      RssTezConfig.RSS_CLIENT_DEFAULT_MAX_SEGMENT_SIZE);
    this.sendThreadNum = conf.getInt(RssTezConfig.RSS_CLIENT_SEND_THREAD_NUM,
      RssTezConfig.RSS_CLIENT_DEFAULT_SEND_THREAD_NUM);
    this.maxBufferSize = conf.getLong(RssTezConfig.RSS_WRITER_BUFFER_SIZE,
      RssTezConfig.RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE);

    this.memoryThreshold = conf.getDouble(RssTezConfig.RSS_CLIENT_MEMORY_THRESHOLD,
      RssTezConfig.RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD);
    this.sendThreshold = conf.getDouble(RssMRConfig.RSS_CLIENT_SEND_THRESHOLD,
      RssMRConfig.RSS_CLIENT_DEFAULT_SEND_THRESHOLD);
    this.batch = this.conf.getInt(RssMRConfig.RSS_CLIENT_BATCH_TRIGGER_NUM,
      RssMRConfig.RSS_CLIENT_DEFAULT_BATCH_TRIGGER_NUM);

    this.sendCheckInterval = conf.getLong(RssMRConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
      RssMRConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE);
    this.sendCheckTimeout = conf.getLong(RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
      RssMRConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    this.bitmapSplitNum = conf.getInt(RssMRConfig.RSS_CLIENT_BITMAP_NUM,
      RssMRConfig.RSS_CLIENT_DEFAULT_BITMAP_NUM);

    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);
    this.codec = Codec.newInstance(this.rssConf);
    this.taskAttemptId = taskAttemptId;      // vertexid + taskid + taskattemptid

    this.sendExecutorService  = Executors.newFixedThreadPool(sendThreadNum,
      ThreadUtils.getThreadFactory("send-thread-%d"));

    this.shuffleWriteClient = RssTezUtils.createShuffleClient(conf);

    String storageType = this.conf.get(RssTezConfig.RSS_STORAGE_TYPE);
    if (StringUtils.isEmpty(storageType)) {
      throw new RssException("storage type mustn't be empty");
    }
    this.isMemoryShuffleEnabled = StorageType.withMemory(StorageType.valueOf(storageType));
    this.sizePerPartition = new long[numPartitions];
  }

  public void addRecord(int partitionId, K key, V value) throws IOException, InterruptedException {
    memoryLock.lock();
    try {
      while (memoryUsedSize.get() > maxMemSize) {
        LOG.warn("memoryUsedSize {} is more than {}, inSendListBytes {}",
          memoryUsedSize, maxMemSize, inSendListBytes);
        full.await();
      }
    } finally {
      memoryLock.unlock();
    }

    if (!buffers.containsKey(partitionId)) {
      SortWriteBuffer<K, V> sortWriterBuffer = new SortWriteBuffer(
        partitionId, comparator, maxSegmentSize, keySerializer, valSerializer);       // TODO: use SortWriteBuffer from MR, change it ???
      buffers.putIfAbsent(partitionId, sortWriterBuffer);
      waitSendBuffers.add(sortWriterBuffer);
    }

    SortWriteBuffer<K, V> buffer = buffers.get(partitionId);
    long length = buffer.addRecord(key, value);
    sizePerPartition[partitionId] += length;
    if (length > maxMemSize) {
      throw new RssException("record is too big");
    }
    LOG.debug("memoryUsedSize {} increase {}", memoryUsedSize, length);
    memoryUsedSize.addAndGet(length);

    // 如果一个当前一个分区的数据大于最大值(默认14M), 则直接发送到shuffle server
    if (buffer.getDataLength() > maxBufferSize) {
      if (waitSendBuffers.remove(buffer)) {
        sendBufferToServers(buffer);
      } else {
        LOG.error("waitSendBuffers don't contain buffer {}", buffer);
      }
    }
    if (memoryUsedSize.get() > maxMemSize * memoryThreshold
      && inSendListBytes.get() <= maxMemSize * sendThreshold) {
      sendBuffersToServers();
    }
    // TODO: Add counter
//    mapOutputRecordCounter.increment(1);
//    mapOutputByteCounter.increment(length);
  }

  public void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Set<ShuffleServerInfo> serverInfos = Sets.newHashSet();
    for (List<ShuffleServerInfo> serverInfoLists : partitionToServers.values()) {
      for (ShuffleServerInfo serverInfo : serverInfoLists) {
        serverInfos.add(serverInfo);
      }
    }

    // TODO: 暂时还没有有效的方式，
    Future<Boolean> future = executor.submit(
      () -> shuffleWriteClient.sendCommit(serverInfos, appId, shuffleId, numTasks));
    long start = System.currentTimeMillis();
    int currentWait = 200;
    int maxWait = 5000;
    while (!future.isDone()) {
      LOG.info("Wait commit to shuffle server for task[" + taskAttemptId + "] cost "
        + (System.currentTimeMillis() - start) + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      // check if commit/finish rpc is successful
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  public void freeAllResources() {
    sendExecutorService.shutdownNow();
  }


  public void waitSendFinished() {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    long start = System.currentTimeMillis();
    while (true) {
      // if failed when send data to shuffle server, mark task as failed
      if (failedBlockIds.size() > 0) {
        String errorMsg =
          "Send failed: failed because " + failedBlockIds.size()
            + " blocks can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }

      // remove blockIds which was sent successfully, if there has none left, all data are sent
      allBlockIds.removeAll(successBlockIds);
      if (allBlockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + allBlockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
          "Timeout: failed because " + allBlockIds.size()
            + " blocks can't be sent to shuffle server in " + sendCheckTimeout + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }

    start = System.currentTimeMillis();
    shuffleWriteClient.reportShuffleResult(partitionToServers, appId, shuffleId,
      taskAttemptId, partitionToBlocks, bitmapSplitNum);      // TODO: reportShuffleResult and finishShuffle的顺序. finishShuffle竟然跑到了reportShuffleResult 之后了。
    long commitDuration = 0;
    if (!isMemoryShuffleEnabled) {
      long s = System.currentTimeMillis();
      sendCommit();
      commitDuration = System.currentTimeMillis() - s;
    }
    LOG.info("Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
      taskAttemptId, bitmapSplitNum, (System.currentTimeMillis() - start));
    LOG.info("Task uncompressed data length {} compress time cost {} ms, commit time cost {} ms,"
        + " copy time cost {} ms, sort time cost {} ms",
      uncompressedDataLen, compressTime, commitDuration, copyTime, sortTime);
  }

  void sendBuffersToServers() {
    waitSendBuffers.sort(new Comparator<SortWriteBuffer<K, V>>() {
      @Override
      public int compare(SortWriteBuffer<K, V> o1, SortWriteBuffer<K, V> o2) {
        return o2.getDataLength() - o1.getDataLength();
      }
    });
    int sendSize = Math.min(batch, waitSendBuffers.size());
    Iterator<SortWriteBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    while (iterator.hasNext() && index < sendSize) {
      SortWriteBuffer buffer = iterator.next();
      prepareBufferForSend(shuffleBlocks, buffer);
      iterator.remove();
      index++;
    }
    sendShuffleBlocks(shuffleBlocks);
  }

  private void sendBufferToServers(SortWriteBuffer<K, V> buffer) {
    List<ShuffleBlockInfo> shuffleBlocks = Lists.newArrayList();
    prepareBufferForSend(shuffleBlocks, buffer);
    sendShuffleBlocks(shuffleBlocks);
  }

  private void prepareBufferForSend(List<ShuffleBlockInfo> shuffleBlocks, SortWriteBuffer buffer) {
    buffers.remove(buffer.getPartitionId());
    ShuffleBlockInfo block = createShuffleBlock(buffer);
    buffer.clear();
    shuffleBlocks.add(block);
    allBlockIds.add(block.getBlockId());
    if (!partitionToBlocks.containsKey(block.getPartitionId())) {
      partitionToBlocks.putIfAbsent(block.getPartitionId(), Lists.newArrayList());
    }
    partitionToBlocks.get(block.getPartitionId()).add(block.getBlockId());
  }

  ShuffleBlockInfo createShuffleBlock(SortWriteBuffer wb) {
    byte[] data = wb.getData();
    copyTime += wb.getCopyTime();
    sortTime += wb.getSortTime();
    int partitionId = wb.getPartitionId();
    final int uncompressLength = data.length;
    long start = System.currentTimeMillis();
    final byte[] compressed = codec.compress(data);
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    compressTime += System.currentTimeMillis() - start;
    final long blockId = RssTezUtils.getBlockId(outputContext, partitionId, getNextSeqNo(partitionId));
    LOG.info("?????? {}, blockid is {}", outputContext.getTaskVertexName(), blockId);
    uncompressedDataLen += data.length;
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(wb.getDataLength());
    return new ShuffleBlockInfo(shuffleId, partitionId, blockId, compressed.length, crc32,
      compressed, partitionToServers.get(partitionId), uncompressLength, wb.getDataLength(), taskAttemptId);
  }

  private void sendShuffleBlocks(List<ShuffleBlockInfo> shuffleBlocks) {
    sendExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        long size = 0;
        try {
          for (ShuffleBlockInfo block : shuffleBlocks) {
            size += block.getFreeMemory();
          }
          // dagId 是否需要考虑attempt???
          SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(appId, shuffleBlocks, () -> false);
          successBlockIds.addAll(result.getSuccessBlockIds());
          failedBlockIds.addAll(result.getFailedBlockIds());
        } catch (Throwable t) {
          LOG.warn("send shuffle data exception ", t);
        } finally {
          try {
            memoryLock.lock();
            LOG.debug("memoryUsedSize {} decrease {}", memoryUsedSize, size);
            memoryUsedSize.addAndGet(-size);
            inSendListBytes.addAndGet(-size);
            full.signalAll();
          } finally {
            memoryLock.unlock();
          }
        }
      }
    });
  }

  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();

  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void checkRssException() {
    if (!failedBlockIds.isEmpty()) {
      throw new RssException("There are some blocks failed");
    }
  }

  public long[] sizePerPartition() {
    return sizePerPartition;
  }

}
