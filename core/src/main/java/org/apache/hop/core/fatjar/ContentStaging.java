/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.fatjar;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

/**
 * Uploads the files a cluster run needs, each named after its content: a file that is already
 * staged with the same size is not uploaded again. After the first run only what changed goes up,
 * typically a small project package instead of a fat jar of several hundred MB.
 *
 * <p>Each file a run uses gets a marker ({@code <name>.lastused}) before it is checked or uploaded,
 * and {@link #keepAlive()} refreshes the markers while the run lasts: {@link StagingCleanup}
 * removes files that no run marked for a while.
 *
 * <p>Used by the native Spark engine in cluster mode and by the Databricks deployment.
 */
public final class ContentStaging {

  /** Prefix of a staged fat jar: {@code hop-native-<sha12>.jar}. */
  public static final String FAT_JAR_PREFIX = "hop-native-";

  /** How often {@link #keepAlive()} refreshes the markers of a running job. */
  static final Duration KEEP_ALIVE_INTERVAL = Duration.ofHours(1);

  private final ILogChannel log;
  private final IStagingStore store;
  private final Set<String> used = new LinkedHashSet<>();
  private Instant lastMarked = Instant.EPOCH;

  public ContentStaging(ILogChannel log, IStagingStore store) {
    this.log = log;
    this.store = store;
  }

  /** The name a fat jar is staged under: {@code hop-native-<first 12 of its sha256>.jar}. */
  public static String fatJarName(Path fatJar) throws HopException {
    return FAT_JAR_PREFIX + FatJarCache.sha256Of(fatJar).substring(0, 12) + ".jar";
  }

  /**
   * Stage the fat jar under its content hash.
   *
   * @return the URL the cluster reads it from
   */
  public String stageFatJar(Path fatJar) throws HopException {
    return stage(fatJar, fatJarName(fatJar));
  }

  /**
   * Stage a local file that is already named after its content.
   *
   * @return the URL the cluster reads it from
   */
  public String stage(Path localFile, String name) throws HopException {
    long size;
    try {
      size = Files.size(localFile);
    } catch (Exception e) {
      throw new HopException("Unable to read " + localFile, e);
    }
    // Mark first: a cleanup that checks the file again before removing it sees this run
    used.add(name);
    mark(name);
    if (store.size(name) == size) {
      log.logBasic("Already staged: " + store.location(name));
    } else {
      long start = System.currentTimeMillis();
      store.upload(localFile, name);
      log.logBasic(
          "Staged "
              + store.location(name)
              + " ("
              + size / 1024
              + " kB in "
              + (System.currentTimeMillis() - start)
              + " ms)");
    }
    return store.clusterUrl(name);
  }

  /** Refresh the markers of the files this run uses, at most once per hour: call while it runs. */
  public void keepAlive() {
    if (Instant.now().isBefore(lastMarked.plus(KEEP_ALIVE_INTERVAL))) {
      return;
    }
    // Also after a failed write: try again in an hour, not on every poll
    lastMarked = Instant.now();
    for (String name : used) {
      mark(name);
    }
  }

  /**
   * Keep a staged file for good: no cleanup removes it, also when no run of Hop marks it as used.
   * For a file that something outside Hop keeps using, such as the fat jar of a Databricks job that
   * runs on the job's own schedule. Never fails: without the sidecar the file is marked as used.
   *
   * @param why what uses the file, written into the sidecar for whoever finds it
   */
  public void keep(String name, String why) {
    try {
      store.writeText(
          name + StagingCleanup.KEEP_SUFFIX,
          why
              + "\nThe Hop staging cleanup never removes "
              + name
              + " while this file is here. Remove this file once nothing uses it any more.\n");
    } catch (HopException e) {
      log.logBasic(
          "Unable to keep "
              + store.location(name)
              + " for good; a staging cleanup may remove it once no run used it for a while: "
              + e.getMessage());
    }
  }

  /** Remove the staged files nobody used for a while, at most once a day; never fails. */
  public void cleanUpIfDue(IVariables variables) {
    StagingCleanup.cleanUpIfDue(log, variables, store);
  }

  private void mark(String name) {
    if (!store.supportsCleanup()) {
      return;
    }
    try {
      Instant now = Instant.now();
      store.writeText(name + StagingCleanup.MARKER_SUFFIX, StagingCleanup.markerText(now));
      lastMarked = now;
    } catch (HopException e) {
      // Only affects cleanup; the run itself does not need it
      log.logDetailed("Unable to mark " + store.location(name) + " as used: " + e.getMessage());
    }
  }
}
