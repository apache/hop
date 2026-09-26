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
package org.apache.hop.ui.hopgui;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;

/**
 * Resolves a writable Hop Web audit root. Per-user data is stored under {@code
 * <root>/users/<username>/}.
 *
 * <p>Does not rely on re-reading {@link Const#HOP_AUDIT_FOLDER} after class init: if the configured
 * path is not writable (common with root-owned bind mounts), falls back to {@code
 * java.io.tmpdir/hop-web-audit}.
 */
public final class HopWebAuditPaths {

  private static final String FALLBACK_DIR_NAME = "hop-web-audit";

  private static volatile String effectiveRoot;

  private HopWebAuditPaths() {}

  /**
   * Effective audit root directory (absolute path). Creates the directory when possible. Thread
   * safe; result is cached for the JVM lifetime.
   *
   * @return writable audit root path
   */
  public static String getAuditRoot() {
    String cached = effectiveRoot;
    if (cached != null) {
      return cached;
    }
    synchronized (HopWebAuditPaths.class) {
      if (effectiveRoot != null) {
        return effectiveRoot;
      }
      effectiveRoot = resolveWritableRoot();
      return effectiveRoot;
    }
  }

  /**
   * Per-user audit root: {@code <auditRoot>/users/<sanitizedUsername>}.
   *
   * @param sanitizedUsername directory-safe username
   * @return absolute path for that user's audit data
   */
  public static String getUserAuditRoot(String sanitizedUsername) {
    return getAuditRoot()
        + Const.FILE_SEPARATOR
        + "users"
        + Const.FILE_SEPARATOR
        + sanitizedUsername;
  }

  /** Clear cache (tests only). */
  static void resetForTests() {
    effectiveRoot = null;
  }

  private static String resolveWritableRoot() {
    String configured = System.getProperty("HOP_AUDIT_FOLDER");
    if (configured == null || configured.isBlank()) {
      // Const may already be initialized from system property / user.dir
      configured = Const.HOP_AUDIT_FOLDER;
    }

    File preferred = tryMakeWritable(configured);
    if (preferred != null) {
      LogChannel.UI.logBasic("Hop Web audit root: " + preferred.getAbsolutePath());
      return preferred.getAbsolutePath();
    }

    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    File fallback = tryMakeWritable(tmp + File.separator + FALLBACK_DIR_NAME);
    if (fallback != null) {
      LogChannel.UI.logBasic(
          "Hop Web audit: configured folder '"
              + configured
              + "' is not writable; using '"
              + fallback.getAbsolutePath()
              + "'");
      return fallback.getAbsolutePath();
    }

    // Last resort: unique under tmpdir
    File unique = new File(tmp, FALLBACK_DIR_NAME + "-" + ProcessHandle.current().pid());
    if (tryMakeWritable(unique.getAbsolutePath()) != null) {
      LogChannel.UI.logBasic(
          "Hop Web audit: using process-unique folder '" + unique.getAbsolutePath() + "'");
      return unique.getAbsolutePath();
    }

    LogChannel.UI.logError(
        "Hop Web audit: unable to create a writable audit folder; using configured path '"
            + configured
            + "' (writes may fail)");
    return configured;
  }

  private static File tryMakeWritable(String path) {
    if (path == null || path.isBlank()) {
      return null;
    }
    try {
      File dir = new File(path).getAbsoluteFile();
      if (!dir.exists() && !dir.mkdirs()) {
        return null;
      }
      if (!dir.isDirectory()) {
        return null;
      }
      File probe = new File(dir, ".hop-write-test");
      Files.writeString(probe.toPath(), "ok");
      //noinspection ResultOfMethodCallIgnored
      probe.delete();
      File users = new File(dir, "users");
      if (!users.exists()) {
        //noinspection ResultOfMethodCallIgnored
        users.mkdirs();
      }
      return dir;
    } catch (IOException | SecurityException e) {
      return null;
    }
  }
}
