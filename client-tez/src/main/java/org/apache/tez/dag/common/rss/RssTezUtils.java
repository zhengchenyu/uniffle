package org.apache.tez.dag.common.rss;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class RssTezUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssTezUtils.class);

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
//    int requiredShuffleServerNumber = con.getInt(
//      RssMRConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER,
//      RssMRConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE
//    );
//    boolean enabledEstimateServer = con.getBoolean(
//      RssMRConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED,
//      RssMRConfig.RSS_ESTIMATE_SERVER_ASSIGNMENT_ENABLED_DEFAULT_VALUE
//    );
//    if (!enabledEstimateServer || requiredShuffleServerNumber > 0) {
//      return requiredShuffleServerNumber;
//    }
//    int taskConcurrency = estimateTaskConcurrency(con);
//    int taskConcurrencyPerServer = con.getInt(RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER,
//      RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_PER_SERVER_DEFAULT_VALUE);
//    return (int) Math.ceil(taskConcurrency * 1.0 / taskConcurrencyPerServer);
    // ZCY TODO: need dynamic number
    return 1;
  }

  public static void buildAssignServers(int reduceId, String[] splitServers,
                                        Collection<ShuffleServerInfo> assignServers) {
    for (String splitServer : splitServers) {
      String[] serverInfo = splitServer.split(":");
      if (serverInfo.length != 2 && serverInfo.length != 3) {
        throw new RssException("partition " + reduceId + " server info isn't right");
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

//  public static int estimateTaskConcurrency(Configuration jobConf) {
//    double dynamicFactor = jobConf.getDouble(RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR,
//      RssMRConfig.RSS_ESTIMATE_TASK_CONCURRENCY_DYNAMIC_FACTOR_DEFAULT_VALUE);
//    double slowStart = jobConf.getDouble(Constants.MR_SLOW_START, Constants.MR_SLOW_START_DEFAULT_VALUE);
//    int mapNum = jobConf.getNumMapTasks();
//    int reduceNum = jobConf.getNumReduceTasks();
//    int mapLimit = jobConf.getInt(Constants.MR_MAP_LIMIT, Constants.MR_MAP_LIMIT_DEFAULT_VALUE);
//    int reduceLimit = jobConf.getInt(Constants.MR_REDUCE_LIMIT, Constants.MR_REDUCE_LIMIT_DEFAULT_VALUE);
//
//    int estimateMapNum = mapLimit > 0 ? Math.min(mapNum, mapLimit) : mapNum;
//    int estimateReduceNum = reduceLimit > 0 ? Math.min(reduceNum, reduceLimit) : reduceNum;
//    if (slowStart == 1) {
//      return (int) (Math.max(estimateMapNum, estimateReduceNum) * dynamicFactor);
//    } else {
//      return (int) (((1 - slowStart) * estimateMapNum + estimateReduceNum) * dynamicFactor);
//    }
//  }

  private static final int MAX_ATTEMPT_LENGTH = 6;
  private static final long MAX_ATTEMPT_ID = (1 << MAX_ATTEMPT_LENGTH) - 1;

  public static long getBlockId(long partitionId, long taskAttemptId, int nextSeqNo) {
    long attemptId = taskAttemptId >> (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH);
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
    long taskId = taskAttemptId - (attemptId
      << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH));
    if (taskId < 0 ||  taskId > Constants.MAX_TASK_ATTEMPT_ID) {
      throw new RssException("Can't support taskId["
        + taskId + "], the max value should be " + Constants.MAX_TASK_ATTEMPT_ID);
    }
    return (atomicInt << (Constants.PARTITION_ID_MAX_LENGTH + Constants.TASK_ATTEMPT_ID_MAX_LENGTH))
      + (partitionId << Constants.TASK_ATTEMPT_ID_MAX_LENGTH) + taskId;
  }
}
