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

package org.apache.hop.vfs.databricks;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.databricks.client.RestDatabricksFilesClient;

/**
 * Maps logical VFS paths ({@code scheme:///input}) to absolute Databricks workspace paths under an
 * optional scheme root (metadata default base path).
 */
public final class DatabricksPathResolver {

  private DatabricksPathResolver() {}

  /**
   * Normalize a configured root path: trim, strip optional {@code dbfs:}, ensure leading slash, no
   * trailing slash (except never return empty for a non-blank input).
   */
  public static String normalizeRootPath(String rootPath) throws HopException {
    if (StringUtils.isBlank(rootPath)) {
      return "";
    }
    String p = RestDatabricksFilesClient.normalizeWorkspacePath(rootPath);
    while (p.length() > 1 && p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    if (!RestDatabricksFilesClient.isFilesApiPath(p)) {
      throw new HopException(
          "Databricks VFS root path must be under /Volumes/… or /Workspace/…, got: " + p);
    }
    return p;
  }

  /**
   * Resolve a logical VFS path to a workspace path.
   *
   * @param logicalPath path from VFS {@link org.apache.commons.vfs2.FileName#getPath()} (e.g.
   *     {@code /input} or {@code /})
   * @param rootPath normalized scheme root, or empty when URIs must be absolute workspace paths
   */
  public static String toWorkspacePath(String logicalPath, String rootPath) {
    String logical = logicalPath;
    if (StringUtils.isBlank(logical) || "/".equals(logical)) {
      logical = "/";
    } else {
      while (logical.length() > 1 && logical.endsWith("/")) {
        logical = logical.substring(0, logical.length() - 1);
      }
      if (!logical.startsWith("/")) {
        logical = "/" + logical;
      }
    }

    String root = StringUtils.isBlank(rootPath) ? "" : rootPath.trim();
    if (StringUtils.isBlank(root)) {
      return logical;
    }

    // Escape hatch: full workspace path in the URI even when a root is configured
    if (RestDatabricksFilesClient.isFilesApiPath(logical)) {
      return logical;
    }

    if ("/".equals(logical)) {
      return root;
    }
    // root has no trailing slash; logical starts with /
    return root + logical;
  }
}
