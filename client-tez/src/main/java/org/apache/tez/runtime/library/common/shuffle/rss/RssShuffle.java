package org.apache.tez.runtime.library.common.shuffle.rss;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.GuavaShim;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.OrderedShufflePlugin;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ExceptionReporter;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.exceptions.InputAlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RssShuffle implements OrderedShufflePlugin, ExceptionReporter {

  private static final Logger LOG = LoggerFactory.getLogger(Shuffle.class);


  InputContext context;
  Configuration conf;
  int numInputs;
  long initialMemoryAvailable;

  private MergeManager merger;

  String appId;
  int shuffleId;
  TaskAttemptID reduceId;


  private AtomicBoolean isShutDown = new AtomicBoolean(false);

  private final long startTime;

  private final ListeningExecutorService executor;
  private final RunShuffleCallable runShuffleCallable;
  private volatile ListenableFuture<TezRawKeyValueIterator> runShuffleFuture;
  RssShuffleScheduler scheduler;

  RssShuffleInputEventHandlerOrderedGrouped eventHandler;

  private final String srcNameTrimmed;

  FileSystem localFS;
  LocalDirAllocator localDirAllocator;
  Combiner combiner;
  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();

  private AtomicBoolean schedulerClosed = new AtomicBoolean(false);
  private AtomicBoolean mergerClosed = new AtomicBoolean(false);

  private String throwingThreadName = null;

  private final TezCounter shufflePhaseTime;
  private final TezCounter mergePhaseTime;


  // Not handling any shutdown logic here. That's handled by the callback from this invocation.
  private class RunShuffleCallable extends CallableWithNdc<TezRawKeyValueIterator> {
    @Override
    protected TezRawKeyValueIterator callInternal() throws IOException, InterruptedException {

      if (!isShutDown.get()) {
        try {
          scheduler.start();
        } catch (Throwable e) {
          throw new ShuffleError("Error during shuffle", e);
        } finally {
          cleanupShuffleScheduler();
        }
      }
      // The ShuffleScheduler may have exited cleanly as a result of a shutdown invocation
      // triggered by a previously reportedException. Check before proceeding further.s
      synchronized (RssShuffle.this) {
        if (throwable.get() != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
            throwable.get());
        }
      }

      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);

      // stop the scheduler
      cleanupShuffleScheduler();

      // Finish the on-going merges...
      TezRawKeyValueIterator kvIter = null;
      context.notifyProgress();
      try {
        kvIter = merger.close(true);
      } catch (Throwable e) {
        // Set the throwable so that future.get() sees the reported errror.
        throwable.set(e);
        throw new ShuffleError("Error while doing final merge ", e);
      }
      mergePhaseTime.setValue(System.currentTimeMillis() - startTime);

      context.notifyProgress();
      // Sanity check
      synchronized (RssShuffle.this) {
        if (throwable.get() != null) {
          throw new ShuffleError("error in shuffle in " + throwingThreadName,
            throwable.get());
        }
      }

      context.inputIsReady();
      LOG.info("merge complete for input vertex : " + srcNameTrimmed);
      return kvIter;
    }
  }



  public RssShuffle(InputContext context, Configuration conf, int numInputs,
                    long initialMemoryAvailable) throws IOException {
    this.context = context;
    this.conf = conf;
    // Ignore numInputs. For auto reduce mode, this is number of task multiply partition count.
    // But in rss mode, numInputs should be partition count.
    this.numInputs = numInputs;
    this.initialMemoryAvailable = initialMemoryAvailable;


    this.appId = context.getApplicationId().toString() + "." + context.getDAGAttemptNumber();
    this.shuffleId = this.conf.getInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "input.vertex.id", -1);
    if (shuffleId == -1) {
      throw new IOException("Config " + RssTezConfig.RSS_ASSIGNMENT_PREFIX + "output.vertex.id is not set!");
    }
    //this.reduceId = context.getTaskVertexIndex();

    startTime = System.currentTimeMillis();

    this.srcNameTrimmed = TezUtilsInternal.cleanVertexName(context.getSourceVertexName());

    if (ConfigUtils.isIntermediateInputCompressed(conf)) {
      Class<? extends CompressionCodec> codecClass =
        ConfigUtils.getIntermediateInputCompressorClass(conf, DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      // Work around needed for HADOOP-12191. Avoids the native initialization synchronization race
      codec.getDecompressorType();
    } else {
      codec = null;
    }
    this.ifileReadAhead = conf.getBoolean(
      TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
      TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
        TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }



    this.localFS = FileSystem.getLocal(this.conf);
    this.localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    this.combiner = TezRuntimeUtils.instantiateCombiner(conf, context);


    // TODO TEZ Get rid of Map / Reduce references.
    this.merger = createMergeManager();      // TODO: 重新构造merger

    this.scheduler = new RssShuffleScheduler(context, conf, numInputs, this.merger, this, appId, shuffleId, startTime,
      srcNameTrimmed, codec, ifileReadAhead, ifileReadAheadLength);

    this.mergePhaseTime = context.getCounters().findCounter(TaskCounter.MERGE_PHASE_TIME);
    this.shufflePhaseTime = context.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

    ExecutorService rawExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).
      setNameFormat("ShuffleAndMergeRunner {" + srcNameTrimmed + "}").build());


    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
    runShuffleCallable = new RunShuffleCallable();

    eventHandler= new RssShuffleInputEventHandlerOrderedGrouped(context, scheduler,
      ShuffleUtils.isTezShuffleHandler(conf));
  }

  @Override
  public void run() throws IOException {        // 启动fetch进程
    merger.configureAndStart();
    runShuffleFuture = executor.submit(runShuffleCallable);
    Futures.addCallback(runShuffleFuture, new ShuffleRunnerFutureCallback(), GuavaShim.directExecutor());
    executor.shutdown();
  }

  @Override
  public void handleEvents(List<Event> events) throws IOException {    // 处理DME，获取partition range
    // TODO: event handler
    if (!isShutDown.get()) {
      eventHandler.handleEvents(events);
    } else {
      LOG.info(srcNameTrimmed + ": " + "Ignoring events since already shutdown. EventCount: " + events.size());
    }
  }

  @Override
  public boolean isInputReady() throws IOException, InterruptedException, TezException {      // 判断shuffle是否完成
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
    }
    if (runShuffleFuture == null) {
      return false;
    }
    // Don't need to check merge status, since runShuffleFuture will only
    // complete once merge is complete.
    return runShuffleFuture.isDone();
  }

  private void handleThrowable(Throwable t) throws IOException, InterruptedException {
    if (t instanceof IOException) {
      throw (IOException) t;
    } else if (t instanceof InterruptedException) {
      throw (InterruptedException) t;
    } else {
      throw new UndeclaredThrowableException(t);
    }
  }

  @Override
  public TezRawKeyValueIterator waitForInput() throws IOException, InterruptedException, TezException {   // 等待shuffle是否完成
    Preconditions.checkState(runShuffleFuture != null,
      "waitForInput can only be called after run");
    TezRawKeyValueIterator kvIter = null;
    try {
      kvIter = runShuffleFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Processor interrupted while waiting for errors, will see an InterruptedException.
      handleThrowable(cause);
    }
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
    }
    return kvIter;
  }

  @Override
  public void shutdown() {          // 清理工作
    if (!isShutDown.getAndSet(true)) {
      // Interrupt so that the scheduler / merger sees this interrupt.
      LOG.info("Shutting down Shuffle for source: " + srcNameTrimmed);
      runShuffleFuture.cancel(true);
      cleanupIgnoreErrors();
    }
  }

  private void cleanupIgnoreErrors() {
    try {
      if (eventHandler != null) {
        eventHandler.logProgress(true);
      }
      try {
        cleanupShuffleSchedulerIgnoreErrors();
      } catch (Exception e) {
        LOG.warn(
          "Error cleaning up shuffle scheduler. Ignoring and continuing with shutdown. Message={}",
          e.getMessage());
      }
      cleanupMerger(true);
    } catch (Throwable t) {
      LOG.info(srcNameTrimmed + ": " + "Error in cleaning up.., ", t);
    }
  }

  private void cleanupShuffleSchedulerIgnoreErrors() {
    try {
      cleanupShuffleScheduler();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(srcNameTrimmed + ": " + "Interrupted while attempting to close the scheduler during cleanup. Ignoring");
    }
  }

  private void cleanupShuffleScheduler() throws InterruptedException {
    if (!schedulerClosed.getAndSet(true)) {
      scheduler.close();
    }
  }

  private void cleanupMerger(boolean ignoreErrors) throws Throwable {
    if (!mergerClosed.getAndSet(true)) {
      try {
        merger.close(false);
      } catch (InterruptedException e) {
        if (ignoreErrors) {
          //Reset the status
          Thread.currentThread().interrupt();
          LOG.info(srcNameTrimmed + ": " + "Interrupted while attempting to close the merger during cleanup. Ignoring");
        } else {
          throw e;
        }
      } catch (Throwable e) {
        if (ignoreErrors) {
          LOG.info(srcNameTrimmed + ": " + "Exception while trying to shutdown merger, Ignoring", e);
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public void reportException(Throwable t) {
    // RunShuffleCallable onFailure deals with ignoring errors on shutdown.
    if (throwable.get() == null) {
      LOG.info(srcNameTrimmed + ": " + "Setting throwable in reportException with message [" + t.getMessage() +
        "] from thread [" + Thread.currentThread().getName());
      throwable.set(t);
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the
      // exception immediately.
      cleanupShuffleSchedulerIgnoreErrors();
    }
  }

  @Override
  public void killSelf(Exception exception, String message) {
    if (!isShutDown.get() && throwable.get() == null) {
      shutdown();
      context.killSelf(exception, message);
    }
  }

  private MergeManager createMergeManager() {
    TezCounter spilledRecordsCounter = context.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter = context.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter = context.getCounters().findCounter(TaskCounter.MERGED_MAP_OUTPUTS);
    return new MergeManager(this.conf, localFS, localDirAllocator, context, combiner, spilledRecordsCounter,
      reduceCombineInputCounter, mergedMapOutputsCounter, this, initialMemoryAvailable, codec, ifileReadAhead,
      ifileReadAheadLength);
  }


  private class ShuffleRunnerFutureCallback implements FutureCallback<TezRawKeyValueIterator> {
    @Override
    public void onSuccess(TezRawKeyValueIterator result) {
      LOG.info(srcNameTrimmed + ": " + "Shuffle Runner thread complete");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutDown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring error");
      } else {
        LOG.error(srcNameTrimmed + ": " + "ShuffleRunner failed with error", t);
        // In case of an abort / Interrupt - the runtime makes sure that this is ignored.
        context.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Runner Failed");
        cleanupIgnoreErrors();
      }
    }
  }

  public static class ShuffleError extends IOException {
    private static final long serialVersionUID = 5753909320586607881L;

    ShuffleError(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
