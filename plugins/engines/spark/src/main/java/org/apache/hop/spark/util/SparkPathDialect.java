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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;

/**
 * Distinguishes <strong>Hop VFS URIs</strong> (Commons VFS / {@code HopVfs}) from
 * <strong>Spark/Hadoop URIs</strong> (Dataset {@code load}/{@code save}, lake PATH).
 *
 * <p>Native Spark file and lake handlers resolve variables, then optionally rewrite the URI scheme
 * using the run configuration <em>path scheme map</em> (for example {@code s3=s3a} or {@code
 * minio=s3a}), then pass the string to Spark. Classic mapPartitions transforms still use Hop VFS
 * unchanged.
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
   * Parse a path scheme map from multi-line text ({@code from=to} per line, {@code #} comments).
   * Tokens may be written as {@code s3}, {@code s3://}, {@code S3A}, etc. — they are normalized to
   * bare lower-case scheme names. Later lines override earlier ones for the same source scheme.
   *
   * @return ordered map sourceScheme → targetScheme; never {@code null}
   */
  public static Map<String, String> parseSchemeMap(String mapText) {
    if (StringUtils.isEmpty(mapText)) {
      return Collections.emptyMap();
    }
    Map<String, String> map = new LinkedHashMap<>();
    for (String line : mapText.split("\\r?\\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      int eq = trimmed.indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String from = normalizeSchemeToken(trimmed.substring(0, eq));
      String to = normalizeSchemeToken(trimmed.substring(eq + 1));
      if (StringUtils.isEmpty(from) || StringUtils.isEmpty(to)) {
        continue;
      }
      map.put(from, to);
    }
    return map.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(map);
  }

  /** Strip optional {@code ://} and lower-case a scheme token from the map text. */
  public static String normalizeSchemeToken(String token) {
    if (StringUtils.isEmpty(token)) {
      return null;
    }
    String t = token.trim();
    if (t.endsWith("://")) {
      t = t.substring(0, t.length() - 3);
    } else if (t.endsWith(":")) {
      t = t.substring(0, t.length() - 1);
    }
    t = t.trim().toLowerCase(Locale.ROOT);
    return t.isEmpty() ? null : t;
  }

  /**
   * Rewrite the URI scheme of {@code path} using the run configuration path scheme map, if any.
   * Paths without a scheme, or whose scheme is not listed, are returned unchanged (after trim).
   */
  public static String toSparkUri(
      String path, ISparkPipelineEngineRunConfiguration runConfiguration) {
    if (runConfiguration == null) {
      return toSparkUri(path, (String) null);
    }
    return toSparkUri(path, runConfiguration.getPathSchemeMap());
  }

  /**
   * Rewrite the URI scheme of {@code path} using multi-line map text ({@code from=to}). Empty or
   * null map leaves the path unchanged (trimmed when non-empty).
   */
  public static String toSparkUri(String path, String schemeMapText) {
    return toSparkUri(path, parseSchemeMap(schemeMapText));
  }

  /**
   * Rewrite the URI scheme of {@code path} when {@code schemeMap} contains an entry for that
   * scheme. Only {@code scheme://…} forms are rewritten (not bare {@code file:/…}).
   */
  public static String toSparkUri(String path, Map<String, String> schemeMap) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    String p = path.trim();
    if (schemeMap == null || schemeMap.isEmpty()) {
      return p;
    }
    String scheme = extractScheme(p);
    if (scheme == null) {
      return p;
    }
    String target = schemeMap.get(scheme);
    if (StringUtils.isEmpty(target) || target.equals(scheme)) {
      return p;
    }
    String prefix = scheme + "://";
    if (p.regionMatches(true, 0, prefix, 0, prefix.length())) {
      return target + "://" + p.substring(prefix.length());
    }
    return p;
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
              + " Hadoop FileSystem URIs — prefer 's3a://…' (or map s3=s3a on the Native Spark run"
              + " configuration path scheme map) with cluster S3A configuration"
              + " (spark.hadoop.fs.s3a.*). Named MinIO/S3 VFS connections need a map entry"
              + " (e.g. minio=s3a) plus S3A endpoint conf. See native Spark paths documentation.";
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
