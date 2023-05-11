package org.apache.tez.dag.app;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import org.apache.tez.dag.records.TezDAGID;
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

  DAGImpl createDAG(DAGProtos.DAGPlan dagPB, TezDAGID dagId) {
    DAGImpl dag = super.createDAG(dagPB, dagId);
    return new RssDAGImpl(dag, this.conf, this.getAppID(), this.getAttemptID());
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
