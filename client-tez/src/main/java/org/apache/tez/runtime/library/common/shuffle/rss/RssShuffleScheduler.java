package org.apache.tez.runtime.library.common.shuffle.rss;


import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.MRIdHelper;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.GuavaShim;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ExceptionReporter;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager;
import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.request.CreateShuffleReadClientRequest;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.UnitConverter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RssShuffleScheduler<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleScheduler.class);

  private final InputContext inputContext;
  private Configuration conf;
  private int numInputs;
  private final int abortFailureLimit;

  private int numPartitions;
  private int shuffleId;
  private String appId;

  private Set<Integer> partitions = Sets.newHashSet();      // the partition set which this vertex need to copy.

  private Map<Integer, Set<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();

  private RemoteStorageInfo remoteStorageInfo;
  private String basePath;
  private String storageType;
  private int readBufferSize;
  private int indexReadLimit;
  private int partitionNumPerRange;

  private volatile Thread shuffleSchedulerThread = null;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  final AtomicInteger remainingInputs;

  final Set<PartitionInput> pendingInputs = new HashSet<PartitionInput>();


  private final Random random = new Random(System.currentTimeMillis());


  private final ExceptionReporter exceptionReporter;

  private CompressionCodec codec;

  private final int numFetchers;
  private final Set<RssFetcherOrderedGrouped> runningFetchers =
    Collections.newSetFromMap(new ConcurrentHashMap<RssFetcherOrderedGrouped, Boolean>());

  private final ListeningExecutorService fetcherExecutor;


  String srcNameTrimmed;

  private final MergeManager merger;

  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private final long startTime;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;


  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private long totalBytesShuffledTillNow = 0;

  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");


  public RssShuffleScheduler(InputContext inputContext,
                             Configuration conf,
                             int numberOfInputs,
                             MergeManager merger,
                             ExceptionReporter exceptionReporter,
                             String appId,
                             int shuffleId,
                             long startTime,
                             String srcNameTrimmed, CompressionCodec codec,
                             boolean ifileReadAhead,
                             int ifileReadAheadLength) {
    this.inputContext = inputContext;
    this.conf = conf;
    this.numInputs = numberOfInputs;      // 这里会要获取的分区的数目。切记需要保证tez在auto reduce下输入正确!!!
    int abortFailureLimitConf = conf.getInt(TezRuntimeConfiguration
      .TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT, TezRuntimeConfiguration
      .TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT_DEFAULT);
    if (abortFailureLimitConf <= -1) {
      abortFailureLimit = Math.max(15, numberOfInputs / 10);
    } else {
      //No upper cap, as user is setting this intentionally
      abortFailureLimit = abortFailureLimitConf;
    }

    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startTime = startTime;
    this.firstEventReceived = inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived = inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);
    this.merger = merger;
    this.exceptionReporter = exceptionReporter;

    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;

    int configuredNumFetchers =
      conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    numFetchers = Math.min(configuredNumFetchers, numInputs);

    this.srcNameTrimmed = srcNameTrimmed;
    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
      TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor = inputContext.createTezFrameworkExecutorService(numFetchers,
        "Fetcher_O {" + srcNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor = Executors.newFixedThreadPool(numFetchers, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("Fetcher_O {" + srcNameTrimmed + "} #%d").build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);


    // 获取shuffle server
    // get assigned RSS servers
    // this.partitionToServers = getAssignedServers(this.conf, this.shuffleId);

    this.basePath = conf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH);
    String remoteStorageConf = conf.get(RssTezConfig.RSS_REMOTE_STORAGE_CONF, "");
    this.remoteStorageInfo = new RemoteStorageInfo(basePath, remoteStorageConf);
    this.storageType = conf.get(RssTezConfig.RSS_STORAGE_TYPE);
    this.readBufferSize = (int) UnitConverter.byteStringAsBytes(conf.get(RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE,
      RssTezConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE));
    this.indexReadLimit = conf.getInt(RssTezConfig.RSS_INDEX_READ_LIMIT, RssTezConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
    this.partitionNumPerRange = this.conf.getInt(RssTezConfig.RSS_PARTITION_NUM_PER_RANGE,
      RssTezConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);

    this.numPartitions = conf.getInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "number", 0);
    if (this.numPartitions < 0) {
      throw new TezUncheckedException("Wrong numPartitions" + this.numPartitions);
    }

    remainingInputs = new AtomicInteger(numberOfInputs);
    // this.code = code;
  }

  public void start() throws Exception {
    shuffleSchedulerThread = Thread.currentThread();
    ShuffleSchedulerCallable schedulerCallable = new ShuffleSchedulerCallable();
    schedulerCallable.call();
  }

  public void close() {
    try {
      if (!isShutdown.getAndSet(true)) {
        try {
          logProgress();
        } catch (Exception e) {
          LOG.warn("Failed log progress while closing, ignoring and continuing shutdown. Message={}",
            e.getMessage());
        }

        // Notify and interrupt the waiting scheduler thread
        synchronized (this) {
          notifyAll();
        }
        // Interrupt the ShuffleScheduler thread only if the close is invoked by another thread.
        // If this is invoked on the same thread, then the shuffleRunner has already complete, and there's
        // no point interrupting it.
        // The interrupt is needed to unblock any merges or waits which may be happening, so that the thread can
        // exit.
        if (shuffleSchedulerThread != null && !Thread.currentThread()
          .equals(shuffleSchedulerThread)) {
          shuffleSchedulerThread.interrupt();
        }

        // Interrupt the fetchers.
        for (RssFetcherOrderedGrouped fetcher : runningFetchers) {
          try {
            fetcher.shutDown();
          } catch (Exception e) {
            LOG.warn(
              "Error while shutting down fetcher. Ignoring and continuing shutdown. Message={}",
              e.getMessage());
          }
        }

//        // Kill the Referee thread.
//        try {
//          referee.interrupt();
//          referee.join();
//        } catch (InterruptedException e) {
//          LOG.warn(
//            "Interrupted while shutting down referee. Ignoring and continuing shutdown");
//          Thread.currentThread().interrupt();
//        } catch (Exception e) {
//          LOG.warn(
//            "Error while shutting down referee. Ignoring and continuing shutdown. Message={}",
//            e.getMessage());
//        }
      }
    } finally {
      long startTime = System.currentTimeMillis();
      if (!fetcherExecutor.isShutdown()) {
        // Ensure that fetchers respond to cancel request.
        fetcherExecutor.shutdownNow();
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Shutting down fetchers for input: {}, shutdown timetaken: {} ms, "
          + "hasFetcherExecutorStopped: {}", srcNameTrimmed,
        (endTime - startTime), hasFetcherExecutorStopped());
    }
  }

  boolean hasFetcherExecutorStopped() {
    return fetcherExecutor.isShutdown();
  }

  protected synchronized  void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }


  public void obsoleteInput(RssInputAttemptIdentifier srcAttempt) {
    // TODO: add logical
  }

  public synchronized void copySucceeded(RssInputAttemptIdentifier srcAttemptIdentifier) throws IOException {
    // TODO: add logical
    remainingInputs.decrementAndGet();
  }


  private Map<RssInputAttemptIdentifier, MutablePair<RssInputAttemptIdentifier, Long>> succeedTaskAttemptIds = Maps.newHashMap();

  public synchronized void addKnownInput(int partitionId, RssInputAttemptIdentifier srcAttempt, long taskAttemptId) {
    if (!this.partitions.contains(partitionId)) {
      this.pendingInputs.add(new PartitionInput(partitionId));
    }
    if (succeedTaskAttemptIds.containsKey(srcAttempt)) {
      // Maybe have different attempt, take the latest attempt
      if (succeedTaskAttemptIds.get(srcAttempt).getLeft().getAttemptNumber() < srcAttempt.getAttemptNumber()) {
        succeedTaskAttemptIds.get(srcAttempt).setLeft(srcAttempt);
        succeedTaskAttemptIds.get(srcAttempt).setRight(taskAttemptId);
      }
    } else {
      succeedTaskAttemptIds.put(srcAttempt, new MutablePair(srcAttempt, taskAttemptId));
    }
  }

  public synchronized Roaring64NavigableMap getSucceedTaskIds() {
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf();
    List<Long> succeedTaskIds = succeedTaskAttemptIds.values().stream().map(mp -> mp.getRight()).
      collect(Collectors.toList());
    for (long id : succeedTaskIds) {
      taskIdBitmap.addLong(id);
    }
    return taskIdBitmap;
  }

  public Set<ShuffleServerInfo> getAssignedServers(int partitionId) {
    Set<ShuffleServerInfo> shuffleServerInfos = partitionToServers.get(partitionId);
    if (shuffleServerInfos == null) {
      String servers = this.conf.get(RssTezConfig.RSS_ASSIGNMENT_PREFIX + shuffleId + "." + partitionId);
      String[] splitServers = servers.split(",");
      Set<ShuffleServerInfo> assignServers = Sets.newHashSet();
      RssTezUtils.buildAssignServers(partitionId, splitServers, assignServers);
      partitionToServers.put(partitionId, assignServers);
    }
    return partitionToServers.get(partitionId);
  }

  private Configuration getRemoteConf() {
    Configuration newConf = new Configuration(conf);
    if (!remoteStorageInfo.isEmpty()) {
      for (Map.Entry<String, String> entry : remoteStorageInfo.getConfItems().entrySet()) {
        newConf.set(entry.getKey(), entry.getValue());
      }
    }
    return newConf;
  }


  private class ShuffleSchedulerCallable extends CallableWithNdc<Void> {


    @Override
    protected Void callInternal() throws InterruptedException, IOException {
      while (!isShutdown.get() && remainingInputs.get() > 0) {
        synchronized (RssShuffleScheduler.this) {
          while ((runningFetchers.size() >= numFetchers || pendingInputs.isEmpty())     // 如果正在运行的fetch已经达到最大数目或没有新的要读取的文件
            && remainingInputs.get() > 0) {     // 如果不存在remainingInputs，即所有的分区都读完了。
            try {
              waitAndNotifyProgress();
            } catch (InterruptedException e) {
              if (isShutdown.get()) {
                LOG.info(srcNameTrimmed + ": " +
                  "Interrupted while waiting for fetchers to complete" +
                  "and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
                Thread.currentThread().interrupt();
                break;
              } else {
                throw e;
              }
            }
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(srcNameTrimmed + ": " + "NumCompletedInputs: {}" + (numInputs - remainingInputs.get()));
        }

        // Ensure there's memory available before scheduling the next Fetcher.
        try {
          // If merge is on, block
          merger.waitForInMemoryMerge();      // TODO: 重新实现
          // In case usedMemory > memorylimit, wait until some memory is released
          merger.waitForShuffleToMergeMemory();      // TODO: 重新实现
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info(srcNameTrimmed + ": " +
              "Interrupted while waiting for merge to complete and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
            Thread.currentThread().interrupt();
            break;
          } else {
            throw e;
          }
        }

        if (!isShutdown.get() && remainingInputs.get() > 0) {
          synchronized (RssShuffleScheduler.this) {
            int numFetchersToRun = numFetchers - runningFetchers.size();
            int count = 0;
            while (count < numFetchersToRun && !isShutdown.get() && remainingInputs.get() > 0) {
              PartitionInput partitionInput;
              try {
                partitionInput = getPartitionInput();  // Leads to a wait.
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(srcNameTrimmed + ": " +
                    "Interrupted while waiting for host and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }
              if (partitionInput == null) {
                break; // Check for the exit condition.
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug(srcNameTrimmed + ": " + "Processing pending host: " + pendingInputs);
              }
              if (!isShutdown.get()) {
                count++;
                if (LOG.isDebugEnabled()) {
                  LOG.debug(srcNameTrimmed + ": " + "Scheduling fetch for inputHost: " +
                    partitionInput.getPartitionId());
                }
                RssFetcherOrderedGrouped fetcher = constructFetcherForHost(partitionInput);
                runningFetchers.add(fetcher);
                ListenableFuture<Void> future = fetcherExecutor.submit(fetcher);
                Futures.addCallback(future, new FetchFutureCallback(fetcher), GuavaShim.directExecutor());
              }
            }
          }
        }
      }
      LOG.info("Shutting down FetchScheduler for input: {}, wasInterrupted={}",
        srcNameTrimmed, Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }

  private synchronized void waitAndNotifyProgress() throws InterruptedException {
    inputContext.notifyProgress();
    wait(1000);
  }


  PartitionInput getPartitionInput() throws InterruptedException {
    while (pendingInputs.isEmpty() && remainingInputs.get() > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("PendingInput=" + pendingInputs);
      }
      waitAndNotifyProgress();
    }

    if (!pendingInputs.isEmpty()) {

      PartitionInput partitionInput = null;
      Iterator<PartitionInput> iter = pendingInputs.iterator();
      int numToPick = random.nextInt(pendingInputs.size());
      for (int i = 0; i <= numToPick; ++i) {
        partitionInput = iter.next();
      }

      pendingInputs.remove(partitionInput);
      partitionInput.markBusy();
      if (LOG.isDebugEnabled()) {
        LOG.debug(srcNameTrimmed + ": " + "Assigning " + partitionInput.getPartitionId() + " to "
          + Thread.currentThread().getName());
      }
      partitionInput.getShuffleStart().set(System.currentTimeMillis());
      return partitionInput;
    } else {
      return null;
    }
  }


  public static class PathPartition {

    final String path;
    final int partition;

    PathPartition(String path, int partition) {
      this.path = path;
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((path == null) ? 0 : path.hashCode());
      result = prime * result + partition;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PathPartition other = (PathPartition) obj;
      if (path == null) {
        if (other.path != null)
          return false;
      } else if (!path.equals(other.path))
        return false;
      if (partition != other.partition)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "PathPartition [path=" + path + ", partition=" + partition + "]";
    }
  }

  RssFetcherOrderedGrouped constructFetcherForHost(PartitionInput partitionInput) throws IOException, InterruptedException {
    int partitionId = partitionInput.getPartitionId();
    ShuffleWriteClient writeClient = RssTezUtils.createShuffleClient(conf);
    Roaring64NavigableMap blockIdBitmap = writeClient.getShuffleResult(
      "", getAssignedServers(partitionId), appId, shuffleId, partitionId);
    writeClient.close();

    // get map-completion events to generate RSS taskIDs
    Roaring64NavigableMap taskIdBitmap = getSucceedTaskIds();

    LOG.info("For shuffleId: " + shuffleId + ", RSS Tez client has fetched blockIds and taskIds successfully");


    // start fetcher to fetch blocks from RSS servers
    if (!taskIdBitmap.isEmpty()) {
      LOG.info("For shuffleId: " + shuffleId + ", Rss Tez client starts to fetch blocks from RSS server");
      Configuration readerConf = getRemoteConf();
      boolean expectedTaskIdsBitmapFilterEnable = getAssignedServers(partitionId).size() > 1;
      CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
        appId, shuffleId, partitionId, storageType, basePath, indexReadLimit, readBufferSize,
        /* partitionNumPerRange 固定为1就好，因为上还有写是一次写一个partition*/ 1, this.numPartitions, blockIdBitmap, taskIdBitmap, new ArrayList<>(getAssignedServers(partitionId)),
        readerConf, new MRIdHelper(), expectedTaskIdsBitmapFilterEnable);
      ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getInstance().createShuffleReadClient(request);
      RssFetcherOrderedGrouped fetcher = new RssFetcherOrderedGrouped(conf, inputContext, partitionInput,  this.merger, this, null /*progress*/, this.codec,
        shuffleReadClient, blockIdBitmap.getLongCardinality(), RssTezConfig.toRssConf(this.conf), ifileReadAhead, ifileReadAheadLength);
      // fetcher.fetchAllRssBlocks();
      LOG.info("In vertex-" + inputContext.getTaskVertexName() + "-" + inputContext.getTaskIndex()
        + ", Rss MR client fetches blocks from RSS server successfully");
      return fetcher;
    }

    // TODO: update progress
    inputContext.notifyProgress();
    return null;
  }

  private void logProgress() {
    int inputsDone = numInputs - remainingInputs.get();
    if (inputsDone > nextProgressLineEventCount.get() || inputsDone == numInputs || isShutdown.get()) {
      nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);      // TODO: 需要更新totalBytesShuffledTillNow
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
      double transferRate = mbs / secsSinceStart;
      LOG.info("copy(" + inputsDone + " of " + numInputs +
        ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) " + mbpsFormat.format(transferRate) + " MB/s)");
    }
  }


  private class FetchFutureCallback implements FutureCallback<Void> {

    private final RssFetcherOrderedGrouped fetcherOrderedGrouped;

    public FetchFutureCallback(RssFetcherOrderedGrouped fetcherOrderedGrouped) {
      this.fetcherOrderedGrouped = fetcherOrderedGrouped;
    }

    private void doBookKeepingForFetcherComplete() {
      synchronized (RssShuffleScheduler.this) {
        runningFetchers.remove(fetcherOrderedGrouped);
        RssShuffleScheduler.this.notifyAll();
      }
    }

    @Override
    public void onSuccess(Void result) {
      fetcherOrderedGrouped.shutDown();
      if (isShutdown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring fetch complete");
      } else {
        doBookKeepingForFetcherComplete();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      fetcherOrderedGrouped.shutDown();
      if (isShutdown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring fetch complete");
      } else {
        LOG.error(srcNameTrimmed + ": " + "Fetcher failed with error", t);
        exceptionReporter.reportException(t);
        doBookKeepingForFetcherComplete();
      }
    }
  }

}
