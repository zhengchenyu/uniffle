package org.apache.tez.runtime.library.common.writers.rss;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

public class RssBaseOrderedPartitionedWriter extends KeyValuesWriter {

  private static final Logger LOG = LoggerFactory.getLogger(RssBaseOrderedPartitionedWriter.class);

  public static final String TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_UNIFFLE_RSS = "uniffle_rss_shuffle";

  protected final OutputContext outputContext;
  protected final Configuration conf;
  protected final int numPartitions;
  protected final long availableMemoryMb;
  final ReportPartitionStats reportPartitionStats;


  protected final Partitioner partitioner;
  protected final Combiner combiner;


  protected final Class keyClass;
  protected final Class valClass;
  protected final SerializationFactory serializationFactory;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  protected final IndexedSorter sorter;

  // Rss write buffer manager
  private WriteBufferManager bufferManager;

  // TODO: 考虑是否有必要? 对于本地shuffle, finalMergeEnabled如果开启的话，会将所有spill的文件在本地合并成一个文件。如果不开启，不合并文件，shuffle在读的时候需要提供spillid的信息来决定读那些问文件。
  // 显然对于rss, finalMergeEnabled是不需要的。但是shuffle server端的多次spill是如何进行合并的呢?????
//  protected final boolean finalMergeEnabled;
  private boolean sendEmptyPartitionDetails;
  private final Deflater deflater;

  private long taskAttemptId;

  // TODO: Add counter
  // ...

  public RssBaseOrderedPartitionedWriter(OutputContext outputContext, Configuration conf, int numOutputs,
                                  long initialMemoryAvailable) throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    this.numPartitions = numOutputs;
    int assignedMb = (int) (initialMemoryAvailable >> 20);
    this.availableMemoryMb = assignedMb;
    this.reportPartitionStats = TezRuntimeConfiguration.ReportPartitionStats.fromString(
      conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
        TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));

    this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    this.combiner = TezRuntimeUtils.instantiateCombiner(this.conf, outputContext);
    this.sorter = ReflectionUtils.newInstance(this.conf.getClass(
      TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS, QuickSort.class,
      IndexedSorter.class), this.conf);

    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerializer = serializationFactory.getSerializer(keyClass);
    valSerializer = serializationFactory.getSerializer(valClass);
    LOG.info(outputContext.getDestinationVertexName() + " using: "
      + "memoryMb=" + assignedMb
      + ", keySerializerClass=" + keyClass
      + ", valueSerializerClass=" + valSerializer
//      + ", comparator=" + (RawComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf)
      + ", partitioner=" + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS)
      + ", serialization=" + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY)
      + ", reportPartitionStats=" + reportPartitionStats);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = createAssignmentMap(this.conf);

    // 这里的taskAttemptId需要与fetchAllRssTaskIds的对应。因此需要是一个全局唯一的, 每个taskattempt对应一个全局唯一的taskattemptid。
    // getTaskVertexIndex是vertexid, getTaskIndex是taskid, getTaskAttemptNumber是task的attemptid
    this.taskAttemptId = RssTezUtils.convertTaskAttemptIdToLong(
      this.outputContext.getTaskVertexIndex(), this.outputContext.getTaskIndex(), this.outputContext.getTaskAttemptNumber());
    LOG.info("!!!!!!!!!!!!!!!!vertex = {}, vertexid = {}, unique task attempt id is {}",
        this.outputContext.getTaskVertexName(), this.outputContext.getTaskVertexIndex(), taskAttemptId);
    this.bufferManager = new WriteBufferManager(this.outputContext, this.conf, true, numPartitions, taskAttemptId, partitionToServers);

//    this.finalMergeEnabled = conf.getBoolean(
//      TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
//      TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);
//    boolean pipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
//      .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
//      .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);
//    if (pipelinedShuffle) {
//      throw new IOException("pipelinedShuffle is not supportted.");
//    }

    String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
      TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_UNIFFLE_RSS);
    if (!auxiliaryService.equals(TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_UNIFFLE_RSS)) {
      throw new IOException("Only " + TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_UNIFFLE_RSS + " is supportted.");
    }

    this.sendEmptyPartitionDetails = conf.getBoolean(
      TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
      TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    deflater = TezCommonUtils.newBestCompressionDeflater();
  }

  private Map<Integer, List<ShuffleServerInfo>> createAssignmentMap(Configuration conf) {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    int vertexId = this.conf.getInt(RssTezConfig.RSS_ASSIGNMENT_SHUFFLE_ID, -1);
    assert vertexId != -1;
    assert vertexId == this.outputContext.getTaskVertexIndex();     // TODO: Remove it! Only for debug.
    for (int i = 0; i < this.numPartitions; i++) {
      String servers = conf.get(RssTezConfig.RSS_ASSIGNMENT_PREFIX + vertexId + "." + i);
      if (StringUtils.isEmpty(servers)) {
        throw new RssException("assign partition " + i + " shouldn't be empty");
      }
      String[] splitServers = servers.split(",");
      List<ShuffleServerInfo> assignServers = Lists.newArrayList();
      RssTezUtils.buildAssignServers(i, splitServers, assignServers);
      partitionToServers.put(i, assignServers);
    }
    return partitionToServers;
  }

  @Override
  public void write(Object key, Iterable<Object> values) throws IOException {
    Iterator<Object> it = values.iterator();
    while(it.hasNext()) {
      write(key, it.next());
    }
  }

  @Override
  public List<Event> close() throws IOException {
    // TODO: report progress
    // reporter.progress();
    bufferManager.freeAllResources();
    return generateEvents();
  }

  private List<Event> generateEvents() throws IOException {
    this.outputContext.notifyProgress();
    List<Event> eventList = Lists.newLinkedList();
    long[]  sizePerPartition = bufferManager.sizePerPartition();

    // 1. Generator VME
    // TODO: 这里可能生成的统计信息不准确。因为开启auto reduce之后，自动计算的reduce的数目与本地shuffle得到的值不同。
    VertexManagerEvent vme = ShuffleUtils.generateVMEvent(outputContext, sizePerPartition, reportDetailedPartitionStats(), deflater);
    eventList.add(vme);

    // 2. generate DME
    BitSet emptyPartitionDetails = new BitSet();
    if (sendEmptyPartitionDetails) {
      for (int i = 0; i < sizePerPartition.length; i++) {
        if (sizePerPartition[i] <= 0) {
          emptyPartitionDetails.set(i);
        }
      }
    }
    CompositeDataMovementEvent csdme = ShuffleUtils.generateDMEvent(null, -1, numPartitions, false, -1, true,
      String.valueOf(taskAttemptId), emptyPartitionDetails, deflater);
    eventList.add(csdme);

    return eventList;
  }

  @Override
  public void flush() throws IOException {
    // TODO: report progress
    // reporter.progress();
    bufferManager.waitSendFinished();
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    try {
      collect(key, value, partitioner.getPartition(key, value, numPartitions));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupt exception", e);
    }
  }

  synchronized void collect(Object key, Object value, final int partition) throws IOException, InterruptedException {   // TODO: synchronized is it necessary?
    // TODO: report progress
    // reporter.progress();

    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
        + keyClass.getName() + ", received "
        + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
        + valClass.getName() + ", received "
        + value.getClass().getName());
    }
    if (partition < 0 || partition >= numPartitions) {
      throw new IOException("Illegal partition for " + key + " (" + partition + ")");
    }
    checkRssException();
    bufferManager.addRecord(partition, key, value);
  }

  private void checkRssException() {
    bufferManager.checkRssException();
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }
}
