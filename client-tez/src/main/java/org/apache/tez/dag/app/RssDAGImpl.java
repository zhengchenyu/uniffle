package org.apache.tez.dag.app;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
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
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class RssDAGImpl extends DAGImpl {

  private static final Logger LOG = LoggerFactory.getLogger(RssDAGImpl.class);
  private Configuration appMasterConf;

  private ApplicationId appId;
  private ApplicationAttemptId appAttemptId;

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

  public RssDAGImpl(TezDAGID dagId, Configuration amConf, DAGProtos.DAGPlan jobPlan, EventHandler eventHandler,
                    TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Credentials dagCredentials,
                    Clock clock, String appUserName, TaskHeartbeatHandler thh, AppContext appContext) {
    super(dagId, amConf, jobPlan, eventHandler, taskCommunicatorManagerInterface, dagCredentials, clock, appUserName,
      thh, appContext);
  }


  public RssDAGImpl(DAGImpl dag, Configuration appMasterConf, ApplicationId appId, ApplicationAttemptId appAttemptId) {
    super(dag.getID(), dag.getConf(), dag.getJobPlan(), dag.getEventHandler(),
      (TaskCommunicatorManagerInterface) getPrivateStaticField("taskCommunicatorManagerInterface", dag),
      dag.getCredentials(), (Clock) getPrivateStaticField("clock", dag), dag.getUserName(),
      (TaskHeartbeatHandler) getPrivateStaticField("taskHeartbeatHandler", dag),
      (AppContext) getPrivateStaticField("appContext", dag));
    this.appMasterConf = appMasterConf;
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    ((StateMachineTez) getStateMachine()).registerStateEnteredCallback(DAGState.INITED, new RssInitialCallback());
  }

  class RssInitialCallback implements OnStateChangedCallback<DAGState, RssDAGImpl> {

    @Override
    public void onStateChanged(RssDAGImpl dag, DAGState state) {
      hookAfterInitDag(dag);
    }
  }

  protected void hookAfterInitDag(DAGImpl dag) {
    String coordinators = appMasterConf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
    ShuffleWriteClient client = RssTezUtils.createShuffleClient(appMasterConf);
    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);

    // Get the configured server assignment tags and it will also add default shuffle version tag.
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = appMasterConf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
    if (StringUtils.isNotEmpty(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    String clientType = appMasterConf.get(RssTezConfig.RSS_CLIENT_TYPE);
    ClientUtils.validateClientType(clientType);
    assignmentTags.add(clientType);

    Configuration extraConf = new Configuration();
    extraConf.clear();

    // get remote storage from coordinator if necessary
    boolean dynamicConfEnabled = appMasterConf.getBoolean(RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
      RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

    // fetch client appMasterConf and apply them if necessary
    if (dynamicConfEnabled) {
      Map<String, String> clusterClientConf = client.fetchClientConf(
        appMasterConf.getInt(RssTezConfig.RSS_ACCESS_TIMEOUT_MS,
          RssTezConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE));
      RssTezUtils.applyDynamicClientConf(extraConf, clusterClientConf);
    }

    String storageType = RssTezUtils.getString(extraConf, appMasterConf, RssTezConfig.RSS_STORAGE_TYPE);
    boolean testMode = RssTezUtils.getBoolean(extraConf, appMasterConf, RssTezConfig.RSS_TEST_MODE_ENABLE, false);
    ClientUtils.validateTestModeConf(testMode, storageType);

    String appId = this.appId.toString() + "." + this.appAttemptId.getAttemptId();
    RemoteStorageInfo defaultRemoteStorage =
      new RemoteStorageInfo(appMasterConf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH, ""));
    RemoteStorageInfo remoteStorage = ClientUtils.fetchRemoteStorage(
      appId, defaultRemoteStorage, dynamicConfEnabled, storageType, client);
    // set the remote storage with actual value
    extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_PATH, remoteStorage.getPath());
    extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_CONF, remoteStorage.getConfString());
    RssTezUtils.validateRssClientConf(extraConf, appMasterConf);

    // When containers have disk with very limited space, reduce is allowed to spill data to hdfs
    if (appMasterConf.getBoolean(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED,
      RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED_DEFAULT)) {

      if (remoteStorage.isEmpty()) {
        throw new IllegalArgumentException("Remote spill only supports "
          + StorageType.MEMORY_LOCALFILE_HDFS.name() + " mode with " + remoteStorage);
      }

      // When remote spill is enabled, reduce task is more easy to crash.
      // We allow more attempts to avoid recomputing job.
      int originalAttempts = appMasterConf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
        TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
      int inc = appMasterConf.getInt(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC,
        RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC_DEFAULT);
      if (inc < 0) {
        throw new IllegalArgumentException(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC
          + " cannot be negative");
      }
      appMasterConf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, originalAttempts + inc);
    }

    int requiredAssignmentShuffleServersNum = RssTezUtils.getRequiredShuffleServerNumber(appMasterConf);

    long retryInterval = appMasterConf.getLong(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
      RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
    int retryTimes = appMasterConf.getInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
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
            try {
              Configuration filteredExtraConf = RssTezConfig.filterRssConf(extraConf);      // kvwriter and shuffle should get rss configuration

              Configuration edgeSourceConf = TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
              edgeSourceConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
              edgeSourceConf.setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "output.vertex.id", sourceVertex.getVertexId().getId());
              edgeSourceConf.addResource(filteredExtraConf);
              edge.getEdgeProperty().getEdgeSource().setUserPayload(TezUtils.createUserPayloadFromConf(edgeSourceConf));

              Configuration edgeDestinationConf = TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
              edgeDestinationConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + sourceVertex.getVertexId().getId() + "." + partitionToServer.getKey(), StringUtils.join(servers, ","));
              edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "input.vertex.id", sourceVertex.getVertexId().getId());
              edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_PREFIX + "number", finalPartitions);
              edgeDestinationConf.addResource(filteredExtraConf);
              edge.getEdgeProperty().getEdgeDestination().setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));
            } catch (IOException e) {
              throw new RssException(e);
            }
          });
        }
      }
    }

    long heartbeatInterval = appMasterConf.getLong(RssTezConfig.RSS_HEARTBEAT_INTERVAL,
      RssTezConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    long heartbeatTimeout = appMasterConf.getLong(RssTezConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
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


  private static Object getPrivateStaticField(String name, Object object) {
    try {
      Field f = object.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.get(object);
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

}
