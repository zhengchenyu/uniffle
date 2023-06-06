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

package org.apache.uniffle.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Sets;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.RetryUtils;
import org.apache.uniffle.common.util.RssUtils;
import sun.security.krb5.Config;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KerberizedHadoop implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KerberizedHadoop.class);

  private MiniKdc kdc;
  private File workDir;
  private Path tempDir;
  private Path kerberizedDfsBaseDir;

  private MiniDFSCluster kerberizedDfsCluster;

  private Class<?> testRunnerCls = KerberizedHadoop.class;

  // The superuser for accessing HDFS
  private String hdfsKeytab;
  private String hdfsPrincipal;
  // The normal user of alex for accessing HDFS
  private String alexKeytab;
  private String alexPrincipal;
  // krb5.conf file path
  private String krb5ConfFile;

  protected void setup() throws Exception {
    tempDir = Files.createTempDirectory("tempDir").toFile().toPath();
    kerberizedDfsBaseDir = Files.createTempDirectory("kerberizedDfsBaseDir").toFile().toPath();

    startKDC();
    LOGGER.info("start kdc success!");
    try {
      startKerberizedDFS();
      LOGGER.info("start kerberizeddfs success!");
    } catch (Throwable t) {
      LOGGER.warn("start kerberizeddfs failed!", t);
      throw new Exception(t);
    }
    setupDFSData();
    LOGGER.info("start dfs data success!");
  }

  private void setupDFSData() throws Exception {
    String principal = "alex/" + RssUtils.getHostIp();
    File keytab = new File(workDir, "alex.keytab");
    kdc.createPrincipal(keytab, principal);
    alexKeytab = keytab.getAbsolutePath();
    alexPrincipal = principal;

    FileSystem writeFs = kerberizedDfsCluster.getFileSystem();
    assertTrue(writeFs.mkdirs(new org.apache.hadoop.fs.Path("/hdfs")));

    boolean ok = writeFs.exists(new org.apache.hadoop.fs.Path("/alex"));
    assertFalse(ok);
    ok = writeFs.mkdirs(new org.apache.hadoop.fs.Path("/alex"));
    assertTrue(ok);

    writeFs.setOwner(new org.apache.hadoop.fs.Path("/alex"), "alex", "alex");
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, false);
    writeFs.setPermission(new org.apache.hadoop.fs.Path("/alex"), permission);

    writeFs.setPermission(new org.apache.hadoop.fs.Path("/"), permission);

    String oneFileContent = "test content";
    FSDataOutputStream fsDataOutputStream =
        writeFs.create(new org.apache.hadoop.fs.Path("/alex/basic.txt"));
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
    br.write(oneFileContent);
    br.close();

    writeFs.setOwner(new org.apache.hadoop.fs.Path("/alex/basic.txt"), "alex", "alex");
    writeFs.setPermission(new org.apache.hadoop.fs.Path("/alex/basic.txt"), permission);
  }

  private Configuration createSecureDFSConfig() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    // https://issues.apache.org/jira/browse/HDFS-7431
    conf.set(DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");

    conf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
        TestDummyImpersonationProvider.class.getName());

    String keystoresDir = kerberizedDfsBaseDir.toFile().getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(testRunnerCls);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    return conf;
  }

  private void startKerberizedDFS() throws Throwable {
    LOGGER.info("start kerberizeddfs 1!");
    String krb5Conf = kdc.getKrb5conf().getAbsolutePath();
    System.setProperty("java.security.krb5.conf", krb5Conf);

    String principal = "hdfs/" + RssUtils.getHostIp();
    File keytab = new File(workDir, "hdfs.keytab");
    kdc.createPrincipal(keytab, principal);
    hdfsKeytab = keytab.getPath();
    hdfsPrincipal = principal + "@" + kdc.getRealm();
    LOGGER.info("start kerberizeddfs 2!");

    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");

    LOGGER.info("kerberos config file is {}", krb5Conf);
    List<String> lines = Files.readAllLines(new File(krb5Conf).toPath());
    for (int i = 0; i < lines.size(); i++) {
      LOGGER.info("KRB FILE line {}, content = {}", i, lines.get(i));
    }

    try {
      ProtectionDomain pd = kdc.getClass().getProtectionDomain();
      CodeSource cs = pd.getCodeSource();
      LOGGER.info("KDC class from {}", cs.getLocation());
      Field field = kdc.getClass().getDeclaredField("simpleKdc");
      field.setAccessible(true);
      SimpleKdcServer simpleKdc = (SimpleKdcServer) field.get(kdc);
      pd = simpleKdc.getClass().getProtectionDomain();
      cs = pd.getCodeSource();
      LOGGER.info("SimpleKdcServer class from {}", cs.getLocation());
      if (System.getProperty("java.vendor").contains("IBM")) {
        throw new RuntimeException("use ibm java here!");
      }
      Field field1 = Config.class.getDeclaredField("singleton");
      field1.setAccessible(true);
      Config c = (Config) field1.get(null);
      if (c == null) {
        LOGGER.info("before c is null");
      } else {
        LOGGER.info("before c is not null");
      }
      Method method = Config.class.getDeclaredMethod("getProperty", String.class);
      method.setAccessible(true);
      String str = (String) method.invoke(null, "java.security.krb5.kdc");
      if (str == null) {
        LOGGER.info("the value of java.security.krb5.kdc is null");
      } else {
        LOGGER.info("the value of java.security.krb5.kdc is not null");
      }
      str = (String) method.invoke(null, "java.security.krb5.realm");
      if (str == null) {
        LOGGER.info("the value of java.security.krb5.realm is null");
      } else {
        LOGGER.info("the value of java.security.krb5.realm is not null");
      }
      str = (String) method.invoke(null, "java.security.krb5.conf");
      if (str == null) {
        LOGGER.info("the value of java.security.krb5.conf is null");
      } else {
        LOGGER.info("the value of java.security.krb5.conf is not null");
      }
      List<String> loadedConfigFile = loadConfigFile(str);
      for (int i = 0; i < loadedConfigFile.size(); i++) {
        LOGGER.info("loadedConfigFile FILE line {}, content = {}", i, lines.get(i));
      }
      Hashtable<String,Object> hashtable = parseStanzaTable(loadedConfigFile);
      for (Map.Entry<String, Object> entry : hashtable.entrySet()) {
        LOGGER.info("hashtable key = {}, value = {}", entry.getKey(), entry.getValue());
      }
      c = Config.getInstance();
      if (c == null) {
        LOGGER.info("after1 c is null");
      } else {
        LOGGER.info("after1 c is not null");
      }
      LOGGER.info("kerberos Config is {}", c);
      Field fieldx = c.getClass().getDeclaredField("stanzaTable");
      fieldx.setAccessible(true);
      Hashtable<String, Object> t = (Hashtable<String, Object>) fieldx.get(c);
      LOGGER.info("stanzaTable is {}", t);
      // LOGGER.info("realm is {}", c.getDefaultRealm());
      LOGGER.info("user.name is {}", System.getProperty("user.home"));
      LOGGER.info("java.version is {}", System.getProperty("java.version"));
      LOGGER.info("os.name is {}", System.getProperty("os.name"));
      LOGGER.info("java.vendor.url is {}", System.getProperty("java.vendor.url"));
      method = c.getClass().getDeclaredMethod("getJavaFileName");
      method.setAccessible(true);
      str = (String) method.invoke(c);
      LOGGER.info("the value of getJavaFileName is {}", str);
      lines = Files.readAllLines(new File(str).toPath());
      for (int i = 0; i < lines.size(); i++) {
        LOGGER.info("JAVA FILE line {}, content = {}", i, lines.get(i));
      }
      method = c.getClass().getDeclaredMethod("get", String[].class);
      method.setAccessible(true);
      String realm = (String) method.invoke(c, new Object[]{new String[]{"libdefaults", "default_realm"}});
      LOGGER.info("realm from reflection invoke is {}", realm);
      method = c.getClass().getDeclaredMethod("useDNS_Realm");
      method.setAccessible(true);
      LOGGER.info("useDNS_Realm is {}", (boolean) method.invoke(c));
//      method = c.getClass().getDeclaredMethod("getRealmFromDNS");
//      method.setAccessible(true);
//      String realmFromDNS = (String) method.invoke(c);
//      LOGGER.info("realmFromDNS is {}", realmFromDNS == null ? "null" : realmFromDNS);
      LOGGER.info("hostname is {}", InetAddress.getLocalHost().getCanonicalHostName());
      
    } catch (Throwable e) {
      LOGGER.info("Found exception when get fields, caused by {}", e);
    }
    
    UserGroupInformation.setConfiguration(conf);
    Field field1 = Config.class.getDeclaredField("singleton");
    field1.setAccessible(true);
    Config c = (Config) field1.get(null);
    if (c == null) {
      LOGGER.info("after c is null");
    } else {
      LOGGER.info("after c is not null");
    }
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    final UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(hdfsPrincipal, hdfsKeytab);

    LOGGER.info("start kerberizeddfs 3!");
    Configuration hdfsConf = createSecureDFSConfig();
    hdfsConf.set("hadoop.proxyuser.hdfs.hosts", "*");
    hdfsConf.set("hadoop.proxyuser.hdfs.groups", "*");
    hdfsConf.set("hadoop.proxyuser.hdfs.users", "*");
    LOGGER.info("start kerberizeddfs 4!");
    this.kerberizedDfsCluster = RetryUtils.retry(() -> {
      List<Integer> ports = findAvailablePorts(5);
      LOGGER.info("Find available ports: {}", ports);

      hdfsConf.set("dfs.datanode.ipc.address", "0.0.0.0:" + ports.get(0));
      hdfsConf.set("dfs.datanode.address", "0.0.0.0:" + ports.get(1));
      hdfsConf.set("dfs.datanode.http.address", "0.0.0.0:" + ports.get(2));
      hdfsConf.set("dfs.datanode.http.address", "0.0.0.0:" + ports.get(3));

      return ugi.doAs(new PrivilegedExceptionAction<MiniDFSCluster>() {

        @Override
        public MiniDFSCluster run() throws Exception {
          LOGGER.info("start kerberizeddfs 5!");
          return new MiniDFSCluster
              .Builder(hdfsConf)
              .nameNodePort(ports.get(4))
              .numDataNodes(1)
              .clusterId("kerberized-cluster-1")
              .checkDataNodeAddrConfig(true)
              .build();
        }
      });
    }, 1000L, 5, Sets.newHashSet(BindException.class));
    LOGGER.info("start kerberizeddfs 6!");
  }

  private List<String> loadConfigFile(final String fileName)
      throws IOException {
    try {
      List<String> v = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new InputStreamReader(
          AccessController.doPrivileged(
              new PrivilegedExceptionAction<FileInputStream> () {
                public FileInputStream run() throws IOException {
                  return new FileInputStream(fileName);
                }
              })))) {
        String line;
        String previous = null;
        while ((line = br.readLine()) != null) {
          line = line.trim();
          if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
            // ignore comments and blank line
            // Comments start with '#' or ';'
            continue;
          }
          // In practice, a subsection might look like:
          //      [realms]
          //      EXAMPLE.COM =
          //      {
          //          kdc = kerberos.example.com
          //          ...
          //      }
          // Before parsed into stanza table, it needs to be
          // converted into a canonicalized style (no indent):
          //      realms = {
          //          EXAMPLE.COM = {
          //              kdc = kerberos.example.com
          //              ...
          //          }
          //      }
          //
          if (line.startsWith("[")) {
            if (!line.endsWith("]")) {
              throw new RuntimeException("Illegal config content:"
                  + line);
            }
            if (previous != null) {
              v.add(previous);
              v.add("}");
            }
            String title = line.substring(
                1, line.length()-1).trim();
            if (title.isEmpty()) {
              throw new RuntimeException("Illegal config content:"
                  + line);
            }
            previous = title + " = {";
          } else if (line.startsWith("{")) {
            if (previous == null) {
              throw new RuntimeException(
                  "Config file should not start with \"{\"");
            }
            previous += " {";
            if (line.length() > 1) {
              // { and content on the same line
              v.add(previous);
              previous = line.substring(1).trim();
            }
          } else {
            // Lines before the first section are ignored
            if (previous != null) {
              v.add(previous);
              previous = line;
            }
          }
        }
        if (previous != null) {
          v.add(previous);
          v.add("}");
        }
      }
      return v;
    } catch (java.security.PrivilegedActionException pe) {
      throw (IOException)pe.getException();
    }
  }

  private Hashtable<String,Object> stanzaTable = new Hashtable<>();

  private Hashtable<String,Object> parseStanzaTable(List<String> v) {
    Hashtable<String,Object> current = stanzaTable;
    for (String line: v) {
      // There are 3 kinds of lines
      // 1. a = b
      // 2. a = {
      // 3. }
      if (line.equals("}")) {
        // Go back to parent, see below
        current = (Hashtable<String,Object>)current.remove(" PARENT ");
        if (current == null) {
          throw new RuntimeException("Unmatched close brace");
        }
      } else {
        int pos = line.indexOf('=');
        if (pos < 0) {
          throw new RuntimeException("Illegal config content:" + line);
        }
        String key = line.substring(0, pos).trim();
        String value = trimmed(line.substring(pos+1));
        if (value.equals("{")) {
          Hashtable<String,Object> subTable;
          if (current == stanzaTable) {
            key = key.toLowerCase(Locale.US);
          }
          subTable = new Hashtable<>();
          current.put(key, subTable);
          // A special entry for its parent. Put whitespaces around,
          // so will never be confused with a normal key
          subTable.put(" PARENT ", current);
          current = subTable;
        } else {
          Vector<String> values;
          if (current.containsKey(key)) {
            Object obj = current.get(key);
            // If a key first shows as a section and then a value,
            // this is illegal. However, we haven't really forbid
            // first value then section, which the final result
            // is a section.
            if (!(obj instanceof Vector)) {
              throw new RuntimeException("Key " + key
                  + "used for both value and section");
            }
            values = (Vector<String>)current.get(key);
          } else {
            values = new Vector<String>();
            current.put(key, values);
          }
          values.add(value);
        }
      }
    }
    if (current != stanzaTable) {
      throw new RuntimeException("Not closed");
    }
    return current;
  }

  private static String trimmed(String s) {
    s = s.trim();
    if (s.length() >= 2 &&
        ((s.charAt(0) == '"' && s.charAt(s.length()-1) == '"') ||
            (s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\''))) {
      s = s.substring(1, s.length()-1).trim();
    }
    return s;
  }

  private void startKDC() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    String hostName = "localhost";
    kdcConf.setProperty(MiniKdc.INSTANCE, "DefaultKrbServer");
    kdcConf.setProperty(MiniKdc.ORG_NAME, "EXAMPLE");
    kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "COM");
    kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName);
    kdcConf.setProperty(MiniKdc.KDC_PORT, "0");
    kdcConf.setProperty(MiniKdc.DEBUG, "true");
    workDir = tempDir.toFile();
    kdc = new MiniKdc(kdcConf, workDir);
    kdc.start();

    krb5ConfFile = kdc.getKrb5conf().getAbsolutePath();
    System.setProperty("java.security.krb5.conf", krb5ConfFile);
    Config.refresh();
  }

  public void tearDown() throws IOException {
    if (kerberizedDfsCluster != null) {
      kerberizedDfsCluster.shutdown(true);
    }
    if (kdc != null) {
      kdc.stop();
    }
    setTestRunner(KerberizedHadoop.class);
    UserGroupInformation.reset();
  }

  private List<Integer> findAvailablePorts(int num) throws IOException {
    List<ServerSocket> sockets = new ArrayList<>();
    List<Integer> ports = new ArrayList<>();

    for (int i = 0; i < num; i++) {
      ServerSocket socket = new ServerSocket(0);
      ports.add(socket.getLocalPort());
      sockets.add(socket);
    }

    for (ServerSocket socket : sockets) {
      socket.close();
    }

    return ports;
  }

  public String getSchemeAndAuthorityPrefix() {
    return kerberizedDfsCluster.getURI().toString() + "/";
  }

  public Configuration getConf() throws IOException {
    Configuration configuration = kerberizedDfsCluster.getFileSystem().getConf();
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
    configuration.set("hadoop.security.authentication", UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    return configuration;
  }

  public FileSystem getFileSystem() throws Exception {
    return kerberizedDfsCluster.getFileSystem();
  }

  public String getHdfsKeytab() {
    return hdfsKeytab;
  }

  public String getHdfsPrincipal() {
    return hdfsPrincipal;
  }

  public String getAlexKeytab() {
    return alexKeytab;
  }

  public String getAlexPrincipal() {
    return alexPrincipal;
  }

  public String getKrb5ConfFile() {
    return krb5ConfFile;
  }

  public MiniKdc getKdc() {
    return kdc;
  }

  /**
   * Should be invoked by extending class to solve the NPE.
   * refer to: https://github.com/apache/hbase/pull/1207
   */
  public void setTestRunner(Class<?> cls) {
    this.testRunnerCls = cls;
  }

  static class TestDummyImpersonationProvider implements ImpersonationProvider {

    @Override
    public void init(String configurationPrefix) {
      // ignore
    }

    /**
     * Allow the user of HDFS can be delegated to alex.
     */
    @Override
    public void authorize(UserGroupInformation userGroupInformation, String s) throws AuthorizationException {
      UserGroupInformation superUser = userGroupInformation.getRealUser();
      LOGGER.info("Proxy: {}", superUser.getShortUserName());
    }

    @Override
    public void setConf(Configuration conf) {
      // ignore
    }

    @Override
    public Configuration getConf() {
      return null;
    }
  }
}
