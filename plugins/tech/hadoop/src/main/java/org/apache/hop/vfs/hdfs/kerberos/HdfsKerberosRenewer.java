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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.logging.LogChannel;

/**
 * Process-wide ticker that re-logs every registered {@link HdfsKerberosSession} from its keytab
 * before the TGT expires. Covers hop-run, hop-server, hop-gui, python and arrow as long as they
 * share the JVM.
 */
public final class HdfsKerberosRenewer {
  private static final HdfsKerberosRenewer INSTANCE = new HdfsKerberosRenewer();

  private final List<HdfsKerberosSession> sessions = new CopyOnWriteArrayList<>();
  private volatile ScheduledExecutorService scheduler;

  private HdfsKerberosRenewer() {}

  public static HdfsKerberosRenewer getInstance() {
    return INSTANCE;
  }

  public void register(HdfsKerberosSession session) {
    if (!sessions.contains(session)) {
      sessions.add(session);
    }
    start();
  }

  synchronized void start() {
    if (scheduler != null) {
      return;
    }
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "hop-hdfs-kerberos-renewer");
              thread.setDaemon(true);
              return thread;
            });
    scheduler.scheduleAtFixedRate(this::renewAll, 1, 1, TimeUnit.MINUTES);
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "hop-hdfs-kerberos-shutdown"));
  }

  void renewAll() {
    for (HdfsKerberosSession session : sessions) {
      try {
        session.renewIfNeeded();
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "HDFS VFS: Kerberos renew failed for " + session.getPrincipal(), e);
      }
    }
  }

  public synchronized void shutdown() {
    ScheduledExecutorService running = scheduler;
    scheduler = null;
    sessions.clear();
    if (running != null) {
      running.shutdownNow();
    }
  }

  List<HdfsKerberosSession> sessions() {
    return sessions;
  }
}
