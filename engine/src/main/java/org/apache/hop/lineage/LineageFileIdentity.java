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

package org.apache.hop.lineage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * Derives the OpenLineage {@code (namespace, name)} identity of a file / object-store dataset from
 * its VFS URI, per the OpenLineage dataset naming specification (<a
 * href="https://openlineage.io/docs/spec/naming">openlineage.io/docs/spec/naming</a>). Kept in the
 * engine (like {@link LineageRelationalIdentity}) so every producer — the OpenLineage sink, and the
 * Beam/Spark engines — derives the <b>same</b> identity for the same physical file, letting a file
 * written by one and read by another reconcile onto one node.
 *
 * <p>The rules by URI scheme:
 *
 * <ul>
 *   <li><b>Local file</b> ({@code file://}): namespace {@code file}, name the absolute path.
 *   <li><b>Object stores</b> ({@code s3://}, {@code gs://}): namespace {@code scheme://bucket},
 *       name the object key (no leading slash).
 *   <li><b>Azure ADLS</b> ({@code abfss://}, {@code wasbs://}): namespace {@code
 *       scheme://container@account…}, name the path (the container lives in the URI authority, not
 *       a credential).
 *   <li><b>Everything else</b> ({@code hdfs://}, {@code ftp://}, {@code sftp://}, {@code
 *       http(s)://} …): namespace {@code scheme://host[:port]}, name the path.
 * </ul>
 *
 * <p>User info (e.g. {@code sftp://user:password@host}) is dropped from the namespace so
 * credentials never leak into lineage — except for Azure, whose authority carries the container,
 * not a secret.
 */
public final class LineageFileIdentity {

  /** Schemes whose authority is a bucket and whose name is the object key with no leading slash. */
  private static final Set<String> OBJECT_STORE_SCHEMES = Set.of("s3", "gs");

  /** Schemes whose authority ({@code container@account}) is location, not a credential. */
  private static final Set<String> AZURE_SCHEMES = Set.of("abfs", "abfss", "wasb", "wasbs");

  private LineageFileIdentity() {}

  /** The OpenLineage {@code (namespace, name)} of a file dataset. */
  public record Identity(String namespace, String name) {}

  /**
   * Derives the identity of the file at {@code uri}, or {@code null} when the URI is blank, has no
   * scheme, or cannot be parsed (the caller then falls back to its legacy naming).
   */
  public static Identity of(String uri) {
    if (StringUtils.isBlank(uri)) {
      return null;
    }
    URI parsed = parse(uri.trim());
    if (parsed == null) {
      return null;
    }
    String scheme = normalizeScheme(parsed.getScheme());
    if (scheme == null) {
      return null;
    }
    String path = parsed.getPath() == null ? "" : parsed.getPath();

    if ("file".equals(scheme)) {
      return new Identity("file", StringUtils.isBlank(path) ? "/" : path);
    }
    if (OBJECT_STORE_SCHEMES.contains(scheme)) {
      String bucket = parsed.getHost();
      String key = stripLeadingSlash(path);
      return StringUtils.isBlank(bucket)
          ? new Identity(scheme, key)
          : new Identity(scheme + "://" + bucket, key);
    }

    String authority = AZURE_SCHEMES.contains(scheme) ? parsed.getAuthority() : hostPort(parsed);
    String namespace = StringUtils.isBlank(authority) ? scheme : scheme + "://" + authority;
    return new Identity(namespace, StringUtils.isBlank(path) ? "/" : path);
  }

  private static URI parse(String uri) {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  private static String normalizeScheme(String rawScheme) {
    if (StringUtils.isBlank(rawScheme)) {
      return null;
    }
    String scheme = rawScheme.trim().toLowerCase(Locale.ROOT);
    return switch (scheme) {
      case "s3", "s3a", "s3n" -> "s3";
      case "gs", "gcs" -> "gs";
      default -> scheme;
    };
  }

  /** Host and port only, so any {@code user:password@} authority is never included. */
  private static String hostPort(URI uri) {
    String host = uri.getHost();
    if (StringUtils.isBlank(host)) {
      return null;
    }
    return uri.getPort() > 0 ? host + ":" + uri.getPort() : host;
  }

  private static String stripLeadingSlash(String path) {
    return path != null && path.startsWith("/") ? path.substring(1) : path;
  }
}
