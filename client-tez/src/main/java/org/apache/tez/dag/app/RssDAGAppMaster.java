package org.apache.tez.dag.app;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.*;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.storage.util.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class RssDAGAppMaster extends DAGAppMaster {

  private static final Logger LOG = LoggerFactory.getLogger(DAGAppMaster.class);

  private Configuration conf;

  final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
      }
    }
  );

  public RssDAGAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId, String nmHost,
                         int nmPort, int nmHttpPort, Clock clock, long appSubmitTime, boolean isSession,
                         String workingDirectory, String[] localDirs, String[] logDirs, String clientVersion,
                         Credentials credentials, String jobUserName,
                         DAGProtos.AMPluginDescriptorProto pluginDescriptorProto) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime, isSession,
      workingDirectory, localDirs, logDirs, clientVersion, credentials, jobUserName, pluginDescriptorProto);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.conf = conf;
  }

  private static void validateInputParam(String value, String param)
    throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  @Override
  protected void hookAfterInitDag(DAGImpl dag) {
    super.hookAfterInitDag(dag);
    String coordinators = conf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
    ShuffleWriteClient client = RssTezUtils.createShuffleClient(conf);
    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);

    // Get the configured server assignment tags and it will also add default shuffle version tag.
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = conf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
    if (StringUtils.isNotEmpty(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    String clientType = conf.get(RssTezConfig.RSS_CLIENT_TYPE);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    Configuration extraConf = new Configuration();
    extraConf.clear();

    // get remote storage from coordinator if necessary
    boolean dynamicConfEnabled = conf.getBoolean(RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
      RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

    // fetch client conf and apply them if necessary
    if (dynamicConfEnabled) {
      Map<String, String> clusterClientConf = client.fetchClientConf(
        conf.getInt(RssTezConfig.RSS_ACCESS_TIMEOUT_MS,
          RssTezConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE));
      RssTezUtils.applyDynamicClientConf(extraConf, clusterClientConf);
    }

    String storageType = RssTezUtils.getString(extraConf, conf, RssTezConfig.RSS_STORAGE_TYPE);
    boolean testMode = RssTezUtils.getBoolean(extraConf, conf, RssTezConfig.RSS_TEST_MODE_ENABLE, false);
    ClientUtils.validateTestModeConf(testMode, storageType);

    String appId = getAppID().toString() + "." + getAttemptID().getAttemptId();
    RemoteStorageInfo defaultRemoteStorage =
      new RemoteStorageInfo(conf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH, ""));
    RemoteStorageInfo remoteStorage = ClientUtils.fetchRemoteStorage(
      appId, defaultRemoteStorage, dynamicConfEnabled, storageType, client);
    // set the remote storage with actual value
    extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_PATH, remoteStorage.getPath());
    extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_CONF, remoteStorage.getConfString());
    RssTezUtils.validateRssClientConf(extraConf, conf);

    // When containers have disk with very limited space, reduce is allowed to spill data to hdfs
    if (conf.getBoolean(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED,
      RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED_DEFAULT)) {

      if (remoteStorage.isEmpty()) {
        throw new IllegalArgumentException("Remote spill only supports "
          + StorageType.MEMORY_LOCALFILE_HDFS.name() + " mode with " + remoteStorage);
      }

      // When remote spill is enabled, reduce task is more easy to crash.
      // We allow more attempts to avoid recomputing job.
      int originalAttempts = conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
        TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
      int inc = conf.getInt(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC,
        RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC_DEFAULT);
      if (inc < 0) {
        throw new IllegalArgumentException(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC
          + " cannot be negative");
      }
      conf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, originalAttempts + inc);
    }

    int requiredAssignmentShuffleServersNum = RssTezUtils.getRequiredShuffleServerNumber(conf);

    long retryInterval = conf.getLong(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
      RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
    int retryTimes = conf.getInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
      RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

    // getShuffleAssignments      # 给shuffle申请资源，这个需要与partiton的个数绑定。该值是一般是设置的分区数。
    // 获取edges, 通过getNumSourceTaskPhysicalOutputs得到partition的个数，然后使用getShuffleAssignments申请资源。
    for (Map.Entry<String, Edge> entry : dag.getEdges().entrySet()) {
      String edgeId = entry.getKey();
      Edge edge = entry.getValue();
      int partitions = 0;
      try {
        partitions = edge.getNumSourceTaskPhysicalOutputs(-1);   // sourceTaskIndex is not used.
      } catch (Exception e) {
      }
      final int finalPartitions = partitions;
      if (partitions > 0) {
        Vertex sourceVertex = dag.getVertex(edge.getSourceVertexName());
//        Vertex destinationVertex = dag.getVertex(edge.getDestinationVertexName());
        ShuffleAssignmentsInfo response;
        try {
          response = RetryUtils.retry(() -> {
            ShuffleAssignmentsInfo shuffleAssignments =
              client.getShuffleAssignments(
                appId,
                Integer.parseInt(edgeId),     // edgeId is String.valueOf(System.identityHashCode(this))
                finalPartitions,
                1,
                Sets.newHashSet(assignmentTags),
                requiredAssignmentShuffleServersNum,
                -1
              );

            Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
              shuffleAssignments.getServerToPartitionRanges();

            if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
              return null;
            }
            LOG.info("Start to register shuffle");
            long start = System.currentTimeMillis();
            serverToPartitionRanges.entrySet().forEach(range -> client.registerShuffle(
              range.getKey(),
              appId,
              sourceVertex.getVertexId().getId(),       // TODO: 这个id是逻辑相关的, 多次运行id是一样的。是否可行? 需要结合tez recovery一起考虑。
              range.getValue(),
              remoteStorage,
              ShuffleDataDistributionType.NORMAL
            ));
            LOG.info("Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");
            return shuffleAssignments;
          }, retryInterval, retryTimes);
        } catch (Throwable throwable) {
          throw new RssException("registerShuffle failed!", throwable);
        }

        if (response != null) {
          response.getPartitionToServers().entrySet().forEach(partitionToServer -> {
            List<String> servers = Lists.newArrayList();
            for (ShuffleServerInfo server : partitionToServer.getValue()) {
              if (server.getNettyPort() > 0) {
                servers.add(server.getHost() + ":" + server.getGrpcPort() + ":" + server.getNettyPort());
              } else {
                servers.add(server.getHost() + ":" + server.getGrpcPort());
              }
            }
//            extraConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + edgeId + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
            try {
              Configuration filteredExtraConf = RssTezConfig.filterRssConf(extraConf);      // kvwriter and shuffle should get rss configuration

              Configuration edgeSourceConf = TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
              edgeSourceConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
              edgeSourceConf.setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "ouput.vertex.id", sourceVertex.getVertexId().getId());
              edgeSourceConf.addResource(filteredExtraConf);
              edge.getEdgeProperty().getEdgeSource().setUserPayload(TezUtils.createUserPayloadFromConf(edgeSourceConf));

              Configuration edgeDestinationConf = TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
              edgeDestinationConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
              edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "input.vertex.id", sourceVertex.getVertexId().getId());
              edgeSourceConf.addResource(filteredExtraConf);
              edge.getEdgeProperty().getEdgeDestination().setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));
            } catch (IOException e) {
              throw new RssException(e);
            }
//            sourceVertex.getConf().set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
//            sourceVertex.getConf().setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "ouput.vertex.id", sourceVertex.getVertexId().getId());
//            destinationVertex.getConf().set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
//            destinationVertex.getConf().setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "input.vertex.id", sourceVertex.getVertexId().getId());
          });
        }
      }
    }

    long heartbeatInterval = conf.getLong(RssTezConfig.RSS_HEARTBEAT_INTERVAL,
      RssTezConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    long heartbeatTimeout = conf.getLong(RssTezConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
    client.registerApplicationInfo(appId, heartbeatTimeout, "user");

    scheduledExecutorService.scheduleAtFixedRate(
      () -> {
        try {
          client.sendAppHeartbeat(appId, heartbeatTimeout);
          LOG.info("Finish send heartbeat to coordinator and servers");
        } catch (Exception e) {
          LOG.warn("Fail to send heartbeat to coordinator and servers", e);
        }
      },
      heartbeatInterval / 2,
      heartbeatInterval,
      TimeUnit.MILLISECONDS);


    // close slow start
    // 是否可以支持slow start
//    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);
//    LOG.warn("close slow start, because RSS does not support it yet");

    // MapReduce don't set setKeepContainersAcrossApplicationAttempts in AppContext, there will be no container
    // to be shared between attempts. Rss don't support shared container between attempts.
    // 是否可以支持recovery
//    conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false);
//    LOG.warn("close recovery enable, because RSS doesn't support it yet");
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      final String pid = System.getenv().get("JVM_PID");
      String containerIdStr =
        System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
        System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr =
        System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      validateInputParam(appSubmitTimeStr,
        ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
        containerId.getApplicationAttemptId();

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      String jobUserName = System
        .getenv(ApplicationConstants.Environment.USER.name());

      // Command line options
      Options opts = new Options();
      opts.addOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION,
        false, "Run Tez Application Master in Session mode");

      CommandLine cliParser = new GnuParser().parse(opts, args);
      boolean sessionModeCliOption = cliParser.hasOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION);

      LOG.info("Creating DAGAppMaster for "
        + "applicationId=" + applicationAttemptId.getApplicationId()
        + ", attemptNum=" + applicationAttemptId.getAttemptId()
        + ", AMContainerId=" + containerId
        + ", jvmPid=" + pid
        + ", userFromEnv=" + jobUserName
        + ", cliSessionOption=" + sessionModeCliOption
        + ", pwd=" + System.getenv(ApplicationConstants.Environment.PWD.name())
        + ", localDirs=" + System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())
        + ", logDirs=" + System.getenv(ApplicationConstants.Environment.LOG_DIRS.name()));

      // TODO Does this really need to be a YarnConfiguration ?
      Configuration conf = new Configuration(new YarnConfiguration());

      DAGProtos.ConfigurationProto confProto =
        TezUtilsInternal.readUserSpecifiedTezConfiguration(System.getenv(ApplicationConstants.Environment.PWD.name()));
      TezUtilsInternal.addUserSpecifiedTezConfiguration(conf, confProto.getConfKeyValuesList());

      DAGProtos.AMPluginDescriptorProto amPluginDescriptorProto = null;
      if (confProto.hasAmPluginDescriptor()) {
        amPluginDescriptorProto = confProto.getAmPluginDescriptor();
      }

      UserGroupInformation.setConfiguration(conf);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

      TezUtilsInternal.setSecurityUtilConfigration(LOG, conf);

      DAGAppMaster appMaster =
        new DAGAppMaster(applicationAttemptId, containerId, nodeHostString,
          Integer.parseInt(nodePortString),
          Integer.parseInt(nodeHttpPortString), new SystemClock(), appSubmitTime,
          sessionModeCliOption,
          System.getenv(ApplicationConstants.Environment.PWD.name()),
          TezCommonUtils.getTrimmedStrings(System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())),
          TezCommonUtils.getTrimmedStrings(System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
          clientVersion, credentials, jobUserName, amPluginDescriptorProto);
      ShutdownHookManager.get().addShutdownHook(
        new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

      // log the system properties
      if (LOG.isInfoEnabled()) {
        String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(conf);
        if (systemPropsToLog != null) {
          LOG.info(systemPropsToLog);
        }
      }

      initAndStartAppMaster(appMaster, conf);

    } catch (Throwable t) {
      LOG.error("Error starting DAGAppMaster", t);
      System.exit(1);
    }
  }
}
