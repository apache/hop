/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.hdfs.kerberos;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.hop.core.Const;
import org.apache.hop.core.auth.kerberos.KerberosUtil;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.HdfsTransport;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

/**
 * One Kerberos login for an HDFS connection. Relies on {@link KerberosUtil} (JAAS keytab / ticket
 * cache), not Hadoop {@code UserGroupInformation} and not a shell {@code kinit}.
 */
public class HdfsKerberosSession {
  private static final Class<?> PKG = HdfsTransport.class;
  private static final double RENEW_AT_FRACTION = 0.80;

  /** Java Kerberos config is process-wide; serialize apply + login + SPNEGO. */
  static final Object JVM_KERBEROS = new Object();

  private final String connectionName;
  private final String principal;
  private final String keytabPath;
  private final boolean useTicketCache;
  private final long fallbackRenewalMillis;
  private final KerberosUtil kerberosUtil;
  private final IVariables variables;
  private final HdfsMeta meta;

  private volatile LoginContext loginContext;
  private volatile long loginTimeMillis;

  public HdfsKerberosSession(IVariables variables, HdfsMeta meta) {
    this(variables, meta, new KerberosUtil());
  }

  HdfsKerberosSession(IVariables variables, HdfsMeta meta, KerberosUtil kerberosUtil) {
    this.connectionName = meta.getName();
    this.principal = variables.resolve(Const.NVL(meta.getPrincipal(), ""));
    this.keytabPath =
        HopVfs.separatorsToUnix(variables.resolve(Const.NVL(meta.getKeytabPath(), "")));
    this.useTicketCache = meta.isUseTicketCache();
    long minutes = Const.toLong(variables.resolve(meta.getRenewalIntervalMinutes()), 360L);
    if (minutes < 1) {
      minutes = 360L;
    }
    this.fallbackRenewalMillis = TimeUnit.MINUTES.toMillis(minutes);
    this.kerberosUtil = kerberosUtil;
    this.variables = variables;
    this.meta = meta;
  }

  public void login() throws LoginException {
    synchronized (JVM_KERBEROS) {
      applyJvmKerberosConfig(variables, meta);
      LoginContext context;
      if (useTicketCache) {
        context = kerberosUtil.getLoginContextFromKerberosCache(principal);
      } else {
        context = kerberosUtil.getLoginContextFromKeytab(principal, keytabPath);
      }
      context.login();
      this.loginContext = context;
      this.loginTimeMillis = System.currentTimeMillis();
      LogChannel.GENERAL.logBasic(BaseMessages.getString(PKG, "Hdfs.Log.KerberosLogin", principal));
      HdfsKerberosRenewer.getInstance().register(this);
    }
  }

  public synchronized void renewIfNeeded() throws LoginException {
    if (loginContext == null) {
      login();
      return;
    }
    if (System.currentTimeMillis() < nextRenewalMillis()) {
      return;
    }
    try {
      loginContext.logout();
    } catch (LoginException e) {
      LogChannel.GENERAL.logDebug("HDFS VFS: logout before renew failed: " + e.getMessage());
    }
    login();
    LogChannel.GENERAL.logBasic(BaseMessages.getString(PKG, "Hdfs.Log.KerberosRenew", principal));
  }

  @SuppressWarnings("removal")
  public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
    synchronized (JVM_KERBEROS) {
      if (loginContext == null) {
        login();
      }
      try {
        return Subject.doAs(loginContext.getSubject(), action);
      } catch (PrivilegedActionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof Exception exception) {
          throw exception;
        }
        throw new Exception(cause);
      }
    }
  }

  public void close() {
    HdfsKerberosRenewer.getInstance().unregister(this);
  }

  public String getPrincipal() {
    return principal;
  }

  String keytabPath() {
    return keytabPath;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public Date ticketEndTime() {
    long millis = ticketEndMillis();
    return millis == 0 ? null : new Date(millis);
  }

  long nextRenewalMillis() {
    long fromTicket = ticketEndMillis();
    if (fromTicket > 0) {
      long lifetime = fromTicket - loginTimeMillis;
      if (lifetime > 0) {
        return loginTimeMillis + (long) (lifetime * RENEW_AT_FRACTION);
      }
    }
    return loginTimeMillis + (long) (fallbackRenewalMillis * RENEW_AT_FRACTION);
  }

  private long ticketEndMillis() {
    LoginContext context = loginContext;
    if (context == null) {
      return 0;
    }
    Set<KerberosTicket> tickets = context.getSubject().getPrivateCredentials(KerberosTicket.class);
    Date latest = null;
    for (KerberosTicket ticket : tickets) {
      if (ticket.getEndTime() != null && (latest == null || ticket.getEndTime().after(latest))) {
        latest = ticket.getEndTime();
      }
    }
    return latest == null ? 0 : latest.getTime();
  }

  static void applyJvmKerberosConfig(IVariables variables, HdfsMeta meta) {
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
    String krb5 = HopVfs.separatorsToUnix(variables.resolve(Const.NVL(meta.getKrb5ConfPath(), "")));
    if (!krb5.isEmpty()) {
      System.setProperty("java.security.krb5.conf", krb5);
      // realm/kdc system properties override the file and caused checksum failures.
      return;
    }
    String realm = variables.resolve(Const.NVL(meta.getRealm(), ""));
    if (!realm.isEmpty()) {
      System.setProperty("java.security.krb5.realm", realm);
    }
    String kdc = variables.resolve(Const.NVL(meta.getKdc(), ""));
    if (!kdc.isEmpty()) {
      System.setProperty("java.security.krb5.kdc", kdc);
    }
  }
}
