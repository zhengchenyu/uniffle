package org.apache.tez.dag.common.rss;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.FastNumberFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

public class RssTezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssTezUtils.class);

  public static final String appAttemptIdStrPrefix = "appattempt";
  private static final String APP_ATTEMPT_ID_PREFIX = appAttemptIdStrPrefix + '_';
  private static final int ATTEMPT_ID_MIN_DIGITS = 6;
  private static final int APP_ID_MIN_DIGITS = 4;

  public static ShuffleWriteClient createShuffleClient(Configuration conf) {
    int heartBeatThreadNum = conf.getInt(RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM,
      RssTezConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax = conf.getInt(RssTezConfig.RSS_CLIENT_RETRY_MAX,
      RssTezConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = conf.getLong(RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
      RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String clientType = conf.get(RssTezConfig.RSS_CLIENT_TYPE,
      RssTezConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int replicaWrite = conf.getInt(RssTezConfig.RSS_DATA_REPLICA_WRITE,
      RssTezConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE);
    int replicaRead = conf.getInt(RssTezConfig.RSS_DATA_REPLICA_READ,
      RssTezConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE);
    int replica = conf.getInt(RssTezConfig.RSS_DATA_REPLICA,
      RssTezConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    boolean replicaSkipEnabled = conf.getBoolean(RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED,
      RssTezConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE);
    int dataTransferPoolSize = conf.getInt(RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE,
      RssTezConfig.RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE);
    int dataCommitPoolSize = conf.getInt(RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE,
      RssTezConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE);
    ShuffleWriteClient client = ShuffleClientFactory
      .getInstance()
      .createShuffleWriteClient(clientType, retryMax, retryIntervalMax,
        heartBeatThreadNum, replica, replicaWrite, replicaRead, replicaSkipEnabled,
        dataTransferPoolSize, dataCommitPoolSize);
    return client;
  }

  public static void applyDynamicClientConf(Configuration conf, Map<String, String> confItems) {
    if (conf == null) {
      LOG.warn("Job conf is null");
      return;
    }

    if (confItems == null || confItems.isEmpty()) {
      LOG.warn("Empty conf items");
      return;
    }

    for (Map.Entry<String, String> kv : confItems.entrySet()) {
      String tezConfKey = kv.getKey();
      if (!tezConfKey.startsWith(RssTezConfig.TEZ_RSS_CONFIG_PREFIX)) {
        tezConfKey = RssTezConfig.TEZ_RSS_CONFIG_PREFIX + tezConfKey;
      }
      String tezConfVal = kv.getValue();
      if (StringUtils.isEmpty(conf.get(tezConfKey, ""))
        || RssTezConfig.RSS_MANDATORY_CLUSTER_CONF.contains(tezConfKey)) {
        LOG.warn("Use conf dynamic conf {} = {}", tezConfKey, tezConfVal);
        conf.set(tezConfKey, tezConfVal);
      }
    }
  }

  public static int getInt(Configuration rssConf, Configuration tezConf, String key, int defaultValue) {
    return rssConf.getInt(key,  tezConf.getInt(key, defaultValue));
  }

  public static long getLong(Configuration rssConf, Configuration tezConf, String key, long defaultValue) {
    return rssConf.getLong(key, tezConf.getLong(key, defaultValue));
  }

  public static boolean getBoolean(Configuration rssConf, Configuration tezConf, String key, boolean defaultValue) {
    return rssConf.getBoolean(key, tezConf.getBoolean(key, defaultValue));
  }

  public static double getDouble(Configuration rssConf, Configuration tezConf, String key, double defaultValue) {
    return rssConf.getDouble(key, tezConf.getDouble(key, defaultValue));
  }

  public static String getString(Configuration rssConf, Configuration tezConf, String key) {
    return rssConf.get(key, tezConf.get(key));
  }

  public static String getString(Configuration rssConf, Configuration tezConf, String key, String defaultValue) {
    return rssConf.get(key, tezConf.get(key, defaultValue));
  }

  public static void validateRssClientConf(Configuration rssConf, Configuration mrConf) {
    int retryMax = getInt(rssConf, mrConf, RssTezConfig.RSS_CLIENT_RETRY_MAX,
      RssTezConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = getLong(rssConf, mrConf, RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
      RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    long sendCheckTimeout = getLong(rssConf, mrConf, RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
      RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    if (retryIntervalMax * retryMax > sendCheckTimeout) {
      throw new IllegalArgumentException(String.format("%s(%s) * %s(%s) should not bigger than %s(%s)",
        RssTezConfig.RSS_CLIENT_RETRY_MAX,
        retryMax,
        RssTezConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
        retryIntervalMax,
        RssTezConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
        sendCheckTimeout));
    }
  }

  public static int getRequiredShuffleServerNumber(Configuration conf) {
    int requiredShuffleServerNumber = conf.getInt(
      RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
      RssTezConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE
    );
    // TODO: estimate task concurrency
    return requiredShuffleServerNumber;
  }

  public static void buildAssignServers(int partitionId, String[] splitServers,
                                        Collection<ShuffleServerInfo> assignServers) {
    for (String splitServer : splitServers) {
      String[] serverInfo = splitServer.split(":");
      if (serverInfo.length != 2 && serverInfo.length != 3) {
        throw new RssException("partition " + partitionId + " server info isn't right");
      }
      ShuffleServerInfo server;
      if (serverInfo.length == 2) {
        server = new ShuffleServerInfo(StringUtils.join(serverInfo, "-"),
          serverInfo[0], Integer.parseInt(serverInfo[1]));
      } else {
        server = new ShuffleServerInfo(StringUtils.join(serverInfo, "-"),
          serverInfo[0], Integer.parseInt(serverInfo[1]), Integer.parseInt(serverInfo[2]));
      }
      assignServers.add(server);
    }
  }

  private static final int MAX_ATTEMPT_LENGTH = 6;
  private static final long MAX_ATTEMPT_ID = (1 << MAX_ATTEMPT_LENGTH) - 1;

  // TODO: getBlockId 和convertTaskAttemptIdToLong进行统一。考虑是否可以与MR中的统一。全局的taskattemptid需要重新命令，因为包括了vertexid, taskid, taskattemptid
  public static long getBlockId(long partitionId, OutputContext context, int nextSeqNo) {
    long attemptId = context.getTaskAttemptNumber();
    if (attemptId < 0 || attemptId > MAX_ATTEMPT_ID) {
      throw new RssException("Can't support attemptId [" + attemptId
        + "], the max value should be " + MAX_ATTEMPT_ID);
    }
    long  atomicInt = (nextSeqNo << MAX_ATTEMPT_LENGTH) + attemptId;
    if (atomicInt < 0 || atomicInt > Constants.MAX_SEQUENCE_NO) {
      throw new RssException("Can't support sequence [" + atomicInt
        + "], the max value should be " + Constants.MAX_SEQUENCE_NO);
    }
    if (partitionId < 0 || partitionId > Constants.MAX_PARTITION_ID) {
      throw new RssException("Can't support partitionId["
        + partitionId + "], the max value should be " + Constants.MAX_PARTITION_ID);
    }
    long taskId = context.getTaskIndex();
    if (taskId < 0 ||  taskId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RssException("Can't support taskId["
        + taskId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
      + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskId;
  }

  private final static int TASK_ATTEMPT_ID_MAX_LENGTH = 8;
  private final static int TASK_ID_MAX_LENGTH = 32;
  private final static int VERTEX_ID_MAX_LENGTH = 16;

  public static final long MAX_TASK_ATTEMPT_ID = (1l << TASK_ATTEMPT_ID_MAX_LENGTH) - 1;
  public static final long MAX_TASK_ID = (1l << TASK_ID_MAX_LENGTH) - 1;
  public static final long MAX_VERTEX_ID = (1l << VERTEX_ID_MAX_LENGTH) - 1;

  public static long convertTaskAttemptIdToLong(int vertexId, int taskId, int taskAttemptId) {
    if (vertexId > MAX_VERTEX_ID) {
      throw new RssException("Vertex " + vertexId +  " exceed");
    }
    if (taskId > MAX_TASK_ID) {
      throw new RssException("Task " + taskId +  " exceed");
    }
    if (taskAttemptId > MAX_TASK_ATTEMPT_ID) {
      throw new RssException("TaskAttempt " + vertexId +  " exceed");
    }
    return (vertexId << (MAX_TASK_ATTEMPT_ID + MAX_TASK_ID)) + taskId << MAX_TASK_ATTEMPT_ID + taskAttemptId;
  }

//  public static RoaringBitmap getPartitionStatsForPhysicalOutput(long[] sizes) {
//    RoaringBitmap partitionStats = new RoaringBitmap();
//    if (sizes == null || sizes.length == 0) {
//      return partitionStats;
//    }
//    final int RANGE_LEN = DATA_RANGE_IN_MB.values().length;
//    for (int i = 0; i < sizes.length; i++) {
//      int bucket = DATA_RANGE_IN_MB.getRange(sizes[i]).ordinal();
//      int index = i * (RANGE_LEN);
//      partitionStats.add(index + bucket);
//    }
//    return partitionStats;
//  }
//
//  @InterfaceAudience.Private
//  public static ByteString compressByteArrayToByteString(byte[] inBytes, Deflater deflater) throws IOException {
//    deflater.reset();
//    ByteString.Output os = ByteString.newOutput();
//    DeflaterOutputStream compressOs = null;
//    try {
//      compressOs = new DeflaterOutputStream(os, deflater);
//      compressOs.write(inBytes);
//      compressOs.finish();
//      ByteString byteString = os.toByteString();
//      return byteString;
//    } finally {
//      if (compressOs != null) {
//        compressOs.close();
//      }
//    }
//  }


  public static Object getPrivateStaticField(String name, Object object) {
    try {
      Field f = object.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(object);
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  public static String constructAppId(ApplicationId applicationId, int appAttemptId) {
    StringBuilder sb = new StringBuilder(64);
    sb.append(APP_ATTEMPT_ID_PREFIX);
    sb.append(applicationId.getClusterTimestamp());
    sb.append('_');
    FastNumberFormat.format(sb, applicationId.getId(), APP_ID_MIN_DIGITS);
    sb.append('_');
    FastNumberFormat.format(sb, appAttemptId, ATTEMPT_ID_MIN_DIGITS);
    return sb.toString();
  }

}
