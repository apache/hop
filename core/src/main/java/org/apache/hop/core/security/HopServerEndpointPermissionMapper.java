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

package org.apache.hop.core.security;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Maps Hop Server servlet paths ({@code /hop/*}) to the {@link Permission} required to call them.
 *
 * <p>Used by the Hop Web authorization filter so the embedded Hop Server API honours the same
 * role-based access control as the RAP UI. Lives in {@code hop-core} so it carries no servlet
 * dependency and can be unit tested in isolation; the servlet filter supplies the request path.
 *
 * <p>Matching is by longest context-path prefix, because most servlets accept trailing path
 * segments (for example {@code /hop/pipelineStatus/<name>/<id>}). Endpoints not listed here are
 * <em>unknown</em>; the filter treats unknown endpoints as default-deny so that new servlets do not
 * silently widen the authenticated attack surface.
 *
 * <p>Every read endpoint maps to {@link Permission#FILE_VIEW} (which the built-in {@code READ_ONLY}
 * role holds), so status and image calls stay available to viewers while mutations and runs do not.
 */
public final class HopServerEndpointPermissionMapper {

  /**
   * Context path → required permission. Insertion order is longest-first only where prefixes would
   * otherwise collide; the lookup does an explicit longest-match so ordering is not load-bearing.
   */
  private static final Map<String, Permission> ENDPOINT_PERMISSIONS = buildTable();

  /** Context path of the JSON API, whose permissions additionally depend on the HTTP method. */
  public static final String API_PREFIX = "/hop/api/v1";

  private static final Map<String, Permission> API_READ_PERMISSIONS = buildApiReadTable();
  private static final Map<String, Permission> API_WRITE_PERMISSIONS = buildApiWriteTable();

  private HopServerEndpointPermissionMapper() {
    // utility
  }

  private static Map<String, Permission> buildTable() {
    Map<String, Permission> map = new LinkedHashMap<>();

    // --- Read / inspect: available to READ_ONLY (FILE_VIEW) ---
    map.put("/hop/status", Permission.FILE_VIEW);
    map.put("/hop/pipelineStatus", Permission.FILE_VIEW);
    map.put("/hop/workflowStatus", Permission.FILE_VIEW);
    map.put("/hop/pipelineImage", Permission.FILE_VIEW);
    map.put("/hop/workflowImage", Permission.FILE_VIEW);
    map.put("/hop/getExecInfo", Permission.FILE_VIEW);
    map.put("/hop/asyncStatus", Permission.FILE_VIEW);
    // Sniffs live rows from a running pipeline: a read of running state, not a mutation.
    map.put("/hop/sniffTransform", Permission.FILE_VIEW);

    // --- Deploy / register a definition on the server: treated as a save/write ---
    map.put("/hop/addPipeline", Permission.FILE_SAVE);
    map.put("/hop/addWorkflow", Permission.FILE_SAVE);
    map.put("/hop/registerPipeline", Permission.FILE_SAVE);
    map.put("/hop/registerWorkflow", Permission.FILE_SAVE);
    map.put("/hop/addExport", Permission.FILE_SAVE);
    map.put("/hop/registerPackage", Permission.FILE_SAVE);

    // --- Execution info store writes/deletes: metadata-level writes ---
    map.put("/hop/registerExecInfo", Permission.METADATA_WRITE);
    map.put("/hop/deleteExecInfo", Permission.METADATA_WRITE);

    // --- Execute: RUN_EXECUTE ---
    map.put("/hop/prepareExec", Permission.RUN_EXECUTE);
    map.put("/hop/startExec", Permission.RUN_EXECUTE);
    map.put("/hop/execPipeline", Permission.RUN_EXECUTE);
    map.put("/hop/execWorkflow", Permission.RUN_EXECUTE);
    map.put("/hop/startPipeline", Permission.RUN_EXECUTE);
    map.put("/hop/startWorkflow", Permission.RUN_EXECUTE);
    map.put("/hop/asyncRun", Permission.RUN_EXECUTE);
    // A web service synchronously executes a pipeline and returns its output.
    map.put("/hop/webService", Permission.RUN_EXECUTE);

    // --- Control a running execution: RUN_STOP ---
    map.put("/hop/stopPipeline", Permission.RUN_STOP);
    map.put("/hop/stopWorkflow", Permission.RUN_STOP);
    map.put("/hop/pausePipeline", Permission.RUN_STOP);

    // --- Remove a deployed definition: FILE_DELETE ---
    map.put("/hop/removePipeline", Permission.FILE_DELETE);
    map.put("/hop/removeWorkflow", Permission.FILE_DELETE);

    return map;
  }

  /**
   * JSON API ({@code /hop/api/v1}) permissions for reading methods (GET).
   *
   * <p>The JSON API carries the verb in the HTTP method rather than in the path, so unlike the
   * servlets above one prefix serves both reads and writes and the two have to be tabulated apart.
   */
  private static Map<String, Permission> buildApiReadTable() {
    Map<String, Permission> map = new LinkedHashMap<>();
    map.put(API_PREFIX + "/metadata", Permission.METADATA_READ);
    // Registry introspection: informational, same level as any other read endpoint.
    map.put(API_PREFIX + "/plugins", Permission.FILE_VIEW);
    // Mirrors /hop/getExecInfo.
    map.put(API_PREFIX + "/location", Permission.FILE_VIEW);
    return map;
  }

  /** JSON API permissions for mutating methods (POST, PUT, PATCH, DELETE). */
  private static Map<String, Permission> buildApiWriteTable() {
    Map<String, Permission> map = new LinkedHashMap<>();
    map.put(API_PREFIX + "/metadata", Permission.METADATA_WRITE);
    // A POST here synchronously executes a pipeline, exactly like /hop/webService.
    map.put(API_PREFIX + "/execute", Permission.RUN_EXECUTE);
    // Mirrors /hop/registerExecInfo and /hop/deleteExecInfo.
    map.put(API_PREFIX + "/location", Permission.METADATA_WRITE);
    // Listing plugins is a GET only; a write here is not a known endpoint.
    return map;
  }

  /**
   * Required permission for a Hop Server request path.
   *
   * @param path servlet path within the app, e.g. {@code /hop/startPipeline} or {@code
   *     /hop/pipelineStatus/name/id}; a leading context path must already be stripped
   * @return the required permission, or empty when the path is not a known Hop Server endpoint
   */
  public static Optional<Permission> requiredPermission(String path) {
    return requiredPermission(null, path);
  }

  /**
   * Required permission for a Hop Server request, taking the HTTP method into account.
   *
   * <p>The {@code /hop/*} servlets encode the operation in the path, so the method is ignored for
   * them. The JSON API under {@value #API_PREFIX} is method-driven: the same path reads with GET
   * and mutates with POST, PUT or DELETE, and the two need different permissions.
   *
   * @param method the HTTP method, or null when it is unknown or irrelevant. An unknown method on
   *     an API path is treated as a mutation, so a new verb cannot land on the read permission.
   * @param path servlet path within the app; a leading context path must already be stripped
   * @return the required permission, or empty when the path is not a known Hop Server endpoint
   */
  public static Optional<Permission> requiredPermission(String method, String path) {
    String normalized = normalize(path);
    if (normalized == null) {
      return Optional.empty();
    }
    if (normalized.equals(API_PREFIX) || normalized.startsWith(API_PREFIX + "/")) {
      Map<String, Permission> table =
          isReadMethod(method) ? API_READ_PERMISSIONS : API_WRITE_PERMISSIONS;
      return longestMatch(table, normalized);
    }
    return longestMatch(ENDPOINT_PERMISSIONS, normalized);
  }

  private static Optional<Permission> longestMatch(
      Map<String, Permission> table, String normalized) {
    Permission best = null;
    int bestLen = -1;
    for (Map.Entry<String, Permission> entry : table.entrySet()) {
      String key = entry.getKey();
      if ((normalized.equals(key) || normalized.startsWith(key + "/")) && key.length() > bestLen) {
        best = entry.getValue();
        bestLen = key.length();
      }
    }
    return Optional.ofNullable(best);
  }

  /** Only GET and HEAD are reads; anything else (including an unknown verb) is a mutation. */
  private static boolean isReadMethod(String method) {
    return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
  }

  /**
   * Whether the path is a known built-in Hop Server endpoint. The filter denies unknown {@code
   * /hop/*} paths by default.
   *
   * @param path servlet path within the app
   * @return true if a built-in endpoint permission is defined for the path
   */
  public static boolean isKnownEndpoint(String path) {
    // A method-less question: known if ANY method could reach it. Asking with a null method alone
    // would resolve API paths against the write table and miss the read-only ones.
    return requiredPermission("GET", path).isPresent()
        || requiredPermission(null, path).isPresent();
  }

  private static String normalize(String path) {
    if (path == null || path.isBlank()) {
      return null;
    }
    String p = path.trim();
    // Strip a ;jsessionid= or matrix params
    int semi = p.indexOf(';');
    if (semi >= 0) {
      p = p.substring(0, semi);
    }
    // Strip a query string if one slipped in
    int q = p.indexOf('?');
    if (q >= 0) {
      p = p.substring(0, q);
    }
    // Drop a trailing slash (but keep a bare "/"). Casing is preserved: the servlet registry keys
    // endpoints case-sensitively (e.g. camelCase "startPipeline"), so the table keys must match
    // exactly on the endpoint segment.
    while (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }
}
