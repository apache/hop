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

package org.apache.hop.spark.util;

import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * Distinguishes <strong>Hop VFS URIs</strong> (Commons VFS / {@code HopVfs}) from
 * <strong>Spark/Hadoop URIs</strong> (Dataset {@code load}/{@code save}, lake PATH).
 *
 * <p>Native Spark file and lake handlers pass paths straight to Spark after variable resolution.
 * Schemes that work in the Hop file dialog (for example {@code s3://} or a named MinIO connection)
 * are not automatically understood by Spark. This class only helps detect common confusions and
 * append clear error hints — it does not rewrite paths.
 *
 * @see org.apache.hop.core.vfs.HopVfs
 */
public final class SparkPathDialect {

  /**
   * Static Hop VFS schemes that commonly confuse operators when pasted into Spark File / Lake path
   * fields. Named VFS connections (MinIO, named S3, etc.) use arbitrary schemes and cannot be
   * listed exhaustively.
   */
  private static final Set<String> KNOWN_HOP_VFS_SCHEMES =
      Set.of("s3", "azure", "azfs", "googledrive", "dropbox", "webdav4", "webdav4s");

  private SparkPathDialect() {}

  /**
   * Extract the URI scheme (lower-case) before {@code ://}, or {@code null} if the path has no
   * scheme. Also recognizes {@code file:/…} (single slash).
   */
  public static String extractScheme(String path) {
    if (StringUtils.isEmpty(path)) {
      return null;
    }
    String p = path.trim();
    int sep = p.indexOf("://");
    if (sep > 0) {
      return p.substring(0, sep).trim().toLowerCase(Locale.ROOT);
    }
    if (p.regionMatches(true, 0, "file:", 0, 5)) {
      return "file";
    }
    return null;
  }

  /**
   * {@code true} when the path uses a known static Hop VFS scheme that is not typical for Spark.
   */
  public static boolean isKnownHopVfsScheme(String path) {
    String scheme = extractScheme(path);
    return scheme != null && KNOWN_HOP_VFS_SCHEMES.contains(scheme);
  }

  /**
   * Human-readable hint for a path that looks like Hop VFS, or {@code null} when no extra guidance
   * applies.
   */
  public static String hopVfsSchemeHint(String path) {
    String scheme = extractScheme(path);
    if (scheme == null || !KNOWN_HOP_VFS_SCHEMES.contains(scheme)) {
      return null;
    }
    return switch (scheme) {
      case "s3" ->
          "'"
              + scheme
              + "://' is a Hop VFS scheme (AWS SDK). Spark File Input/Output and Lake PATH use"
              + " Hadoop FileSystem URIs — prefer 's3a://…' with cluster S3A configuration"
              + " (spark.hadoop.fs.s3a.*). Named MinIO/S3 VFS connections are not Spark schemes;"
              + " use s3a:// plus endpoint conf instead. See native Spark paths documentation.";
      case "azure", "azfs" ->
          "'"
              + scheme
              + "://' is a Hop VFS scheme. On Spark, Azure object storage is usually 'abfs://' or"
              + " 'wasbs://' with Hadoop Azure connector configuration — not Hop VFS.";
      case "googledrive", "dropbox", "webdav4", "webdav4s" ->
          "'"
              + scheme
              + "://' is a Hop VFS scheme and is not a standard Spark/Hadoop FileSystem URI."
              + " Spark File/Lake paths must be readable by every executor via Hadoop FS"
              + " (file:///, hdfs://, s3a://, abfs://, …).";
      default ->
          "'"
              + scheme
              + "://' looks like a Hop VFS scheme. Spark Dataset I/O does not use Hop VFS;"
              + " use a Spark/Hadoop URI such as file:///, hdfs://, or s3a://.";
    };
  }

  /**
   * Append a Hop-vs-Spark path hint to an error message when the path uses a known Hop-only scheme.
   * Leaves the message unchanged otherwise.
   */
  public static String withPathHint(String message, String path) {
    String hint = hopVfsSchemeHint(path);
    if (hint == null) {
      return message;
    }
    if (StringUtils.isEmpty(message)) {
      return hint;
    }
    return message + " Hint: " + hint;
  }
}
