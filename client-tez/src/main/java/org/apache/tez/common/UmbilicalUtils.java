/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.ShuffleServerInfo;

import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_ADDRESS;
import static org.apache.tez.common.RssTezConfig.RSS_AM_SHUFFLE_MANAGER_PORT;

public class UmbilicalUtils {
  private static final Logger LOG = LoggerFactory.getLogger(UmbilicalUtils.class);

  private static UmbilicalUtils INSTANCE;
  private TezRemoteShuffleUmbilicalProtocol umbilical;
  private String host;
  private int port;

  private UmbilicalUtils(Configuration conf, ApplicationId applicationId) {
    try {
      this.host = conf.get(RSS_AM_SHUFFLE_MANAGER_ADDRESS);
      this.port = conf.getInt(RSS_AM_SHUFFLE_MANAGER_PORT, -1);
      final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);

      UserGroupInformation taskOwner =
          UserGroupInformation.createRemoteUser(applicationId.toString());
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
      SecurityUtil.setTokenService(jobToken, address);
      taskOwner.addToken(jobToken);
      this.umbilical =
          taskOwner.doAs(
              new PrivilegedExceptionAction<TezRemoteShuffleUmbilicalProtocol>() {
                @Override
                public TezRemoteShuffleUmbilicalProtocol run() throws Exception {
                  return RPC.getProxy(
                      TezRemoteShuffleUmbilicalProtocol.class,
                      TezRemoteShuffleUmbilicalProtocol.versionID,
                      address,
                      conf);
                }
              });
    } catch (Exception e) {
      throw new RssException(e);
    }
  }

  public static synchronized UmbilicalUtils getInstance(Configuration conf, ApplicationId applicationId) {
    if (INSTANCE == null) {
      INSTANCE = new UmbilicalUtils(conf, applicationId);
    }
    return INSTANCE;
  }

  /**
   * @param applicationId Application Id of this task
   * @param conf Configuration
   * @param taskAttemptId task Attempt Id
   * @param shuffleId computed using dagId, up dagName, down dagName by
   *     RssTezUtils.computeShuffleId() method.
   * @return Shuffle Server Info by request Application Master
   * @throws IOException
   * @throws InterruptedException
   * @throws TezException
   */
  private Map<Integer, List<ShuffleServerInfo>> doRequestShuffleServer(
      ApplicationId applicationId,
      Configuration conf,
      TezTaskAttemptID taskAttemptId,
      int shuffleId)
      throws IOException, InterruptedException, TezException {
    GetShuffleServerRequest request =
        new GetShuffleServerRequest(taskAttemptId, 200, 200, shuffleId);

    GetShuffleServerResponse response = this.umbilical.getShuffleAssignments(request);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        response
            .getShuffleAssignmentsInfoWritable()
            .getShuffleAssignmentsInfo()
            .getPartitionToServers();
    LOG.info(
        "RequestShuffleServer applicationId:{}, taskAttemptId:{}, host:{}, port:{}, shuffleId:{}, worker:{}",
        applicationId,
        taskAttemptId,
        host,
        port,
        shuffleId,
        partitionToServers);
    return partitionToServers;
  }

  public Map<Integer, List<ShuffleServerInfo>> requestShuffleServer(
      ApplicationId applicationId,
      Configuration conf,
      TezTaskAttemptID taskAttemptId,
      int shuffleId) {
    try {
      return doRequestShuffleServer(applicationId, conf, taskAttemptId, shuffleId);
    } catch (IOException | InterruptedException | TezException e) {
      LOG.error(
          "Failed to requestShuffleServer, applicationId:{}, taskAttemptId:{}, shuffleId:{}, worker:{}",
          applicationId,
          taskAttemptId,
          shuffleId,
          e);
    }
    return null;
  }

  public void reportShuffleFetchFailure(String appId, int shuffleId, int stageAttemptId, int partitionId, 
                                               String exception) throws IOException {
    RssReportShuffleFetchFailureRequest request =
        new RssReportShuffleFetchFailureRequest(appId, shuffleId, stageAttemptId, partitionId, exception);
    this.umbilical.reportShuffleFetchFailure(request);
  }
}
