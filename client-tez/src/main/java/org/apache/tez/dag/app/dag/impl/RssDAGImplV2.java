package org.apache.tez.dag.app.dag.impl;

import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START;
import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.common.rss.RssTezConfig;
import org.apache.tez.dag.common.rss.RssTezUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.util.ClientUtils;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.storage.util.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RssDAGImplV2 extends DAGImpl {

  private static final Logger LOG = LoggerFactory.getLogger(RssDAGImplV2.class);
  private Configuration amConf;
  String appId;

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

  public RssDAGImplV2(TezDAGID dagId, Configuration amConf, DAGProtos.DAGPlan jobPlan, EventHandler eventHandler,
                      TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Credentials dagCredentials,
                      Clock clock, String appUserName, TaskHeartbeatHandler thh, AppContext appContext) {
    super(dagId, amConf, jobPlan, eventHandler, taskCommunicatorManagerInterface, dagCredentials, clock, appUserName,
      thh, appContext);
  }


  public RssDAGImplV2(DAGImpl dag, Configuration appMasterConf, String appId) {
    super(dag.getID(), dag.getConf(), dag.getJobPlan(), dag.getEventHandler(),
      (TaskCommunicatorManagerInterface) RssTezUtils.getPrivateStaticField("taskCommunicatorManagerInterface", dag),
      dag.getCredentials(), (Clock) RssTezUtils.getPrivateStaticField("clock", dag), dag.getUserName(),
      (TaskHeartbeatHandler) RssTezUtils.getPrivateStaticField("taskHeartbeatHandler", dag),
      (AppContext) RssTezUtils.getPrivateStaticField("appContext", dag));
    this.amConf = appMasterConf;
    this.appId = appId;
    ((StateMachineTez) getStateMachine()).registerStateEnteredCallback(DAGState.INITED, new RssDAGInitedCaledlback());
  }

  ShuffleWriteClient client;
  Set<String> assignmentTags;
  int requiredAssignmentShuffleServersNum;
  RemoteStorageInfo remoteStorage;
  long retryInterval;
  int retryTimes;
  Configuration extraConf;

  class RssDAGInitedCaledlback implements OnStateChangedCallback<DAGState, RssDAGImplV2> {

    @Override
    public void onStateChanged(RssDAGImplV2 dag, DAGState state) {
      try {
        String coordinators = amConf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
        client = RssTezUtils.createShuffleClient(amConf);
        LOG.info("Registering coordinators {}", coordinators);
        client.registerCoordinators(coordinators);

        // Get the configured server assignment tags and it will also add default shuffle version tag.
        assignmentTags = new HashSet<>();
        String rawTags = amConf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
        if (StringUtils.isNotEmpty(rawTags)) {
          rawTags = rawTags.trim();
          assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
        }
        assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
        String clientType = amConf.get(RssTezConfig.RSS_CLIENT_TYPE);
        ClientUtils.validateClientType(clientType);
        assignmentTags.add(clientType);

        extraConf = new Configuration();
        extraConf.clear();

        // get remote storage from coordinator if necessary
        boolean dynamicConfEnabled = amConf.getBoolean(RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED,
          RssTezConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED_DEFAULT_VALUE);

        // fetch client appMasterConf and apply them if necessary
        if (dynamicConfEnabled) {
          Map<String, String> clusterClientConf = client.fetchClientConf(
            amConf.getInt(RssTezConfig.RSS_ACCESS_TIMEOUT_MS,
              RssTezConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE));
          RssTezUtils.applyDynamicClientConf(extraConf, clusterClientConf);
        }

        String storageType = RssTezUtils.getString(extraConf, amConf, RssTezConfig.RSS_STORAGE_TYPE);
        boolean testMode = RssTezUtils.getBoolean(extraConf, amConf, RssTezConfig.RSS_TEST_MODE_ENABLE, false);
        ClientUtils.validateTestModeConf(testMode, storageType);

        RemoteStorageInfo defaultRemoteStorage =
          new RemoteStorageInfo(amConf.get(RssTezConfig.RSS_REMOTE_STORAGE_PATH, ""));
        remoteStorage = ClientUtils.fetchRemoteStorage(
          appId, defaultRemoteStorage, dynamicConfEnabled, storageType, client);
        // set the remote storage with actual value
        extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_PATH, remoteStorage.getPath());
        extraConf.set(RssTezConfig.RSS_REMOTE_STORAGE_CONF, remoteStorage.getConfString());
        RssTezUtils.validateRssClientConf(extraConf, amConf);

        // When containers have disk with very limited space, reduce is allowed to spill data to hdfs
        if (amConf.getBoolean(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED,
          RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ENABLED_DEFAULT)) {

          if (remoteStorage.isEmpty()) {
            throw new IllegalArgumentException("Remote spill only supports "
              + StorageType.MEMORY_LOCALFILE_HDFS.name() + " mode with " + remoteStorage);
          }

          // When remote spill is enabled, reduce task is more easy to crash.
          // We allow more attempts to avoid recomputing job.
          int originalAttempts = amConf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
          int inc = amConf.getInt(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC,
            RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC_DEFAULT);
          if (inc < 0) {
            throw new IllegalArgumentException(RssTezConfig.RSS_REDUCE_REMOTE_SPILL_ATTEMPT_INC
              + " cannot be negative");
          }
          amConf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, originalAttempts + inc);
        }

        requiredAssignmentShuffleServersNum = RssTezUtils.getRequiredShuffleServerNumber(amConf);

        retryInterval = amConf.getLong(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
          RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
        retryTimes = amConf.getInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
          RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

        registerVertexInited(dag);

//        long heartbeatInterval = amConf.getLong(RssTezConfig.RSS_HEARTBEAT_INTERVAL,
//          RssTezConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
        long heartbeatInterval = amConf.getLong(RssTezConfig.RSS_HEARTBEAT_INTERVAL, 100000);   // TODO: this is test, change it later...

        long heartbeatTimeout = amConf.getLong(RssTezConfig.RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);
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
        boolean slowStartEnabled = amConf.getBoolean(TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START,
            TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT);
        if (slowStartEnabled) {
          amConf.setBoolean(TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START, false);
          LOG.warn("close slow start, because RSS does not support it yet");
        }
      } catch (Throwable e) {
        LOG.error("Register rss information failed, caused by {}", e);
        throw new TezUncheckedException(e);
      }
    }

    void registerVertexInited(DAGImpl dag) {
      for (Map.Entry<String, Edge> entry : dag.getEdges().entrySet()) {
        Edge edge = entry.getValue();
        Vertex sourceVertex = dag.getVertex(edge.getSourceVertexName());
        Vertex destinationVertex = dag.getVertex(edge.getDestinationVertexName());
        StateMachineTez stateMachine =
            (StateMachineTez) RssTezUtils.getPrivateStaticField("stateMachine", destinationVertex);
        stateMachine.registerStateEnteredCallback(VertexState.INITED, new RssVertexInitedCaledlback());
      }
    }
  }

  class RssVertexInitedCaledlback implements OnStateChangedCallback<VertexState, VertexImpl> {

    @Override
    public void onStateChanged(VertexImpl vertex, VertexState vertexState) {
      for (Map.Entry<Vertex, Edge> entry : vertex.sourceVertices.entrySet()) {
        try {
          VertexImpl sourceVertex = (VertexImpl) entry.getKey();
          Edge edge = entry.getValue();
          int shuffleId = sourceVertex.getVertexId().getId();
          int numPartitions = edge.getSourceSpec(-1).getPhysicalEdgeCount();
          if (numPartitions > 0) {
            ShuffleAssignmentsInfo response;
            try {
              response = RetryUtils.retry(() -> {
                ShuffleAssignmentsInfo shuffleAssignments =
                    client.getShuffleAssignments(
                        appId,
                        shuffleId,
                        numPartitions,
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
                    shuffleId,
                    range.getValue(),
                    remoteStorage,
                    ShuffleDataDistributionType.NORMAL
                ));
                LOG.info("Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");
                return shuffleAssignments;
              }, retryInterval, retryTimes);

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

                  // Inform the shuffle server information and rss config to output and input
                  try {
                    Configuration filteredExtraConf = RssTezUtils.filterRssConf(extraConf);

                    Configuration edgeSourceConf =
                        TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
                    edgeSourceConf.set(RssTezConfig.RSS_ASSIGNMENT_PREFIX + shuffleId + "." + partitionToServer.getKey(),
                        StringUtils.join(servers, ","));
                    edgeSourceConf.setInt(RssTezConfig.RSS_ASSIGNMENT_SHUFFLE_ID, shuffleId);
                    edgeSourceConf.addResource(filteredExtraConf);
                    edge.getEdgeProperty().getEdgeSource()
                        .setUserPayload(TezUtils.createUserPayloadFromConf(edgeSourceConf));

                    Configuration edgeDestinationConf =
                        TezUtils.createConfFromUserPayload(edge.getEdgeProperty().getEdgeSource().getUserPayload());
                    edgeDestinationConf.set(
                        RssTezConfig.RSS_ASSIGNMENT_PREFIX + shuffleId + "." + partitionToServer.getKey(),
                        StringUtils.join(servers, ","));
                    edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_SHUFFLE_ID, shuffleId);
                    edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_NUM_PARTITIONS, numPartitions);
                    edgeDestinationConf.addResource(filteredExtraConf);
                    edge.getEdgeProperty().getEdgeDestination()
                        .setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));
                  } catch (IOException e) {
                    throw new RssException(e);
                  }
                });
              }
            } catch (Throwable throwable) {
              throw new RssException("registerShuffle failed!", throwable);
            }
          }
        } catch (AMUserCodeException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
