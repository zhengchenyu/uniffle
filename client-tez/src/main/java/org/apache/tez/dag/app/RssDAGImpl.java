package org.apache.tez.dag.app;

import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START;
import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START_DEFAULT;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
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


public class RssDAGImpl extends DAGImpl {

  private static final Logger LOG = LoggerFactory.getLogger(RssDAGImpl.class);
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

  public RssDAGImpl(TezDAGID dagId, Configuration amConf, DAGProtos.DAGPlan jobPlan, EventHandler eventHandler,
                    TaskCommunicatorManagerInterface taskCommunicatorManagerInterface, Credentials dagCredentials,
                    Clock clock, String appUserName, TaskHeartbeatHandler thh, AppContext appContext) {
    super(dagId, amConf, jobPlan, eventHandler, taskCommunicatorManagerInterface, dagCredentials, clock, appUserName,
      thh, appContext);
  }


  public RssDAGImpl(DAGImpl dag, Configuration appMasterConf, String appId) {
    super(dag.getID(), dag.getConf(), dag.getJobPlan(), dag.getEventHandler(),
      (TaskCommunicatorManagerInterface) RssTezUtils.getPrivateStaticField("taskCommunicatorManagerInterface", dag),
      dag.getCredentials(), (Clock) RssTezUtils.getPrivateStaticField("clock", dag), dag.getUserName(),
      (TaskHeartbeatHandler) RssTezUtils.getPrivateStaticField("taskHeartbeatHandler", dag),
      (AppContext) RssTezUtils.getPrivateStaticField("appContext", dag));
    this.amConf = appMasterConf;
    this.appId = appId;
    ((StateMachineTez) getStateMachine()).registerStateEnteredCallback(DAGState.INITED, new RssInitialCallback());
  }

  class RssInitialCallback implements OnStateChangedCallback<DAGState, RssDAGImpl> {

    @Override
    public void onStateChanged(RssDAGImpl dag, DAGState state) {
      try {
        String coordinators = amConf.get(RssTezConfig.RSS_COORDINATOR_QUORUM);
        ShuffleWriteClient client = RssTezUtils.createShuffleClient(amConf);
        LOG.info("Registering coordinators {}", coordinators);
        client.registerCoordinators(coordinators);

        // Get the configured server assignment tags and it will also add default shuffle version tag.
        Set<String> assignmentTags = new HashSet<>();
        String rawTags = amConf.get(RssTezConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
        if (StringUtils.isNotEmpty(rawTags)) {
          rawTags = rawTags.trim();
          assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
        }
        assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
        String clientType = amConf.get(RssTezConfig.RSS_CLIENT_TYPE);
        ClientUtils.validateClientType(clientType);
        assignmentTags.add(clientType);

        Configuration extraConf = new Configuration();
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
        RemoteStorageInfo remoteStorage = ClientUtils.fetchRemoteStorage(
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

        int requiredAssignmentShuffleServersNum = RssTezUtils.getRequiredShuffleServerNumber(amConf);

        long retryInterval = amConf.getLong(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL,
          RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_INTERVAL_DEFAULT_VALUE);
        int retryTimes = amConf.getInt(RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES,
          RssTezConfig.RSS_CLIENT_ASSIGNMENT_RETRY_TIMES_DEFAULT_VALUE);

        // After DAG is inited, the getNumSourceTaskPhysicalOutputs of edgeManager is just the number of partitions.
        // Note: Though tez.shuffle-vertex-manager.enable.auto-parallel may be enable, the number of partiton will never
        //       change, but the number of reduce may change.
        for (Map.Entry<String, Edge> entry : dag.getEdges().entrySet()) {
          Edge edge = entry.getValue();
          int partitions = edge.getNumSourceTaskPhysicalOutputs(-1);   // sourceTaskIndex is not used.
          if (partitions > 0) {
            Vertex sourceVertex = dag.getVertex(edge.getSourceVertexName());
            // Regard the id of source vertex as shuffle id.
            int shuffleId = sourceVertex.getVertexId().getId();
            ShuffleAssignmentsInfo response;
            try {
              response = RetryUtils.retry(() -> {
                ShuffleAssignmentsInfo shuffleAssignments =
                  client.getShuffleAssignments(
                    appId,
                    shuffleId,
                    partitions,
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
                  edgeDestinationConf.setInt(RssTezConfig.RSS_ASSIGNMENT_NUM_PARTITIONS, partitions);
                  edgeDestinationConf.addResource(filteredExtraConf);
                  edge.getEdgeProperty().getEdgeDestination()
                      .setUserPayload(TezUtils.createUserPayloadFromConf(edgeDestinationConf));
                } catch (IOException e) {
                  throw new RssException(e);
                }
              });
            }
          }
        }

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
  }

}
