package org.apache.tez.dag.common.rss;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.config.RssConf;

import java.util.Map;
import java.util.Set;

public class RssTezConfig {

  public static final String TEZ_RSS_CONFIG_PREFIX = "tez.";

  public static final String RSS_COORDINATOR_QUORUM =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_COORDINATOR_QUORUM;

  public static final String RSS_CLIENT_HEARTBEAT_THREAD_NUM =
    TEZ_RSS_CONFIG_PREFIX + "rss.client.heartBeat.threadNum";
  public static final int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;

  public static final String RSS_CLIENT_RETRY_MAX = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_MAX;
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE;

  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX;
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE;

  public static final String RSS_CLIENT_TYPE = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_TYPE;
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE = RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE;

  public static final String RSS_CLIENT_SEND_CHECK_TIMEOUT_MS =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS;
  public static final long RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE;

  public static final String RSS_DATA_REPLICA_WRITE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_WRITE;
  public static final int RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE =
    RssClientConfig.RSS_DATA_REPLICA_WRITE_DEFAULT_VALUE;

  public static final String RSS_DATA_REPLICA_READ =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_READ;
  public static final int RSS_DATA_REPLICA_READ_DEFAULT_VALUE =
    RssClientConfig.RSS_DATA_REPLICA_READ_DEFAULT_VALUE;

  public static final String RSS_DATA_REPLICA_SKIP_ENABLED =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED;

  public static final boolean RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE =
    RssClientConfig.RSS_DATA_REPLICA_SKIP_ENABLED_DEFAULT_VALUE;

  public static final String RSS_DATA_REPLICA = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_REPLICA;
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE = RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE;

  public static final String RSS_DATA_TRANSFER_POOL_SIZE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_TRANSFER_POOL_SIZE;
  public static final int RSS_DATA_TRANSFER_POOL_SIZE_DEFAULT_VALUE =
    RssClientConfig.RSS_DATA_TRANFER_POOL_SIZE_DEFAULT_VALUE;

  public static final String RSS_DATA_COMMIT_POOL_SIZE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE;
  public static final int RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE =
    RssClientConfig.RSS_DATA_COMMIT_POOL_SIZE_DEFAULT_VALUE;

  public static final String RSS_CLIENT_ASSIGNMENT_TAGS =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_TAGS;

  public static final String RSS_DYNAMIC_CLIENT_CONF_ENABLED =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED;
  public static final boolean RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE =
    RssClientConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE;

  public static final String RSS_ACCESS_TIMEOUT_MS = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_ACCESS_TIMEOUT_MS;
  public static final int RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE = RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE;

  public static final String RSS_STORAGE_TYPE = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_STORAGE_TYPE;
  public static final String RSS_REMOTE_STORAGE_PATH =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_REMOTE_STORAGE_PATH;
  public static final String RSS_REMOTE_STORAGE_CONF =
    TEZ_RSS_CONFIG_PREFIX + "rss.remote.storage.conf";

  public static final Set<String> RSS_MANDATORY_CLUSTER_CONF =
    ImmutableSet.of(RSS_STORAGE_TYPE, RSS_REMOTE_STORAGE_PATH);

  //Whether enable test mode for the MR Client
  public static final String RSS_TEST_MODE_ENABLE = TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_TEST_MODE_ENABLE;

  public static final String RSS_REDUCE_REMOTE_SPILL_ENABLED = TEZ_RSS_CONFIG_PREFIX
    + "rss.reduce.remote.spill.enable";
  public static final boolean RSS_REDUCE_REMOTE_SPILL_ENABLED_DEFAULT = false;
  public static final String RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC = TEZ_RSS_CONFIG_PREFIX
    + "rss.reduce.remote.spill.attempt.inc";
  public static final int RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC_DEFAULT = 1;
  public static final String RSS_REDUCE_REMOTE_SPILL_REPLICATION = TEZ_RSS_CONFIG_PREFIX
    + "rss.reduce.remote.spill.replication";
  public static final int RSS_REDUCE_REMOTE_SPILL_REPLICATION_DEFAULT = 1;
  public static final String RSS_REDUCE_REMOTE_SPILL_RETRIES = TEZ_RSS_CONFIG_PREFIX
    + "rss.reduce.remote.spill.retries";
  public static final int RSS_REDUCE_REMOTE_SPILL_RETRIES_DEFAULT = 5;

  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL;
  public static final long RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE;

  public static final String RSS_CLIENT_ASSIGNMENT_RETRY_TIMES =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES;
  public static final int RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE;

  public static final String RSS_HEARTBEAT_INTERVAL =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_INTERVAL;
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE =
    RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE;
  public static final String RSS_HEARTBEAT_TIMEOUT =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_HEARTBEAT_TIMEOUT;

  public static final String RSS_CLIENT_MAX_SEGMENT_SIZE =
    TEZ_RSS_CONFIG_PREFIX + "rss.client.max.buffer.size";
  public static final long RSS_CLIENT_DEFAULT_MAX_SEGMENT_SIZE = 3 * 1024;

  public static final String RSS_CLIENT_SEND_THREAD_NUM =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_SEND_THREAD_NUM;
  public static final int RSS_CLIENT_DEFAULT_SEND_THREAD_NUM =
    RssClientConfig.RSS_CLIENT_DEFAULT_SEND_NUM;

  public static final String RSS_WRITER_BUFFER_SIZE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_WRITER_BUFFER_SIZE;
  public static final long RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = 1024 * 1024 * 14;

  public static final String RSS_CLIENT_MEMORY_THRESHOLD =
    TEZ_RSS_CONFIG_PREFIX + "rss.client.memory.threshold";
  public static final double RSS_CLIENT_DEFAULT_MEMORY_THRESHOLD = 0.8f;

  public static final String RSS_CLIENT_READ_BUFFER_SIZE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE;

  // When the size of read buffer reaches the half of JVM region (i.e., 32m),
  // it will incur humongous allocation, so we set it to 14m.
  public static final String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE;

  public static final String RSS_INDEX_READ_LIMIT =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_INDEX_READ_LIMIT;
  public static final int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE =
    RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE;

  public static final String RSS_PARTITION_NUM_PER_RANGE =
    TEZ_RSS_CONFIG_PREFIX + RssClientConfig.RSS_PARTITION_NUM_PER_RANGE;
  public static final int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE =
    RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE;

  public static final String RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER =
    RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER;
  public static final int RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE =
    RssClientConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER_DEFAULT_VALUE;


  // Assignment
  public static final String RSS_ASSIGNMENT_PREFIX = TEZ_RSS_CONFIG_PREFIX + "rss.assignment.partition.";
  public static final String RSS_ASSIGNMENT_SHUFFLE_ID = RssTezConfig.RSS_ASSIGNMENT_PREFIX + "output.vertex.id";
  public static final String RSS_ASSIGNMENT_NUM_PARTITIONS = RssTezConfig.RSS_ASSIGNMENT_PREFIX + "number.partitions";


  public static RssConf toRssConf(Configuration tezConf) {
    RssConf rssConf = new RssConf();
    for (Map.Entry<String, String> entry : tezConf) {
      String key = entry.getKey();
      if (!key.startsWith(TEZ_RSS_CONFIG_PREFIX)) {
        continue;
      }
      key = key.substring(TEZ_RSS_CONFIG_PREFIX.length());
      rssConf.setString(key, entry.getValue());
    }
    return rssConf;
  }

  public static Configuration filterRssConf(Configuration extraConf) {
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : extraConf) {
      String key = entry.getKey();
      if (key.startsWith(TEZ_RSS_CONFIG_PREFIX)) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return conf;
  }

}
