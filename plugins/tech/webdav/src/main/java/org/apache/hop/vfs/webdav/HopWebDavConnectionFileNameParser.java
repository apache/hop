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
package org.apache.hop.vfs.webdav;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileNameParser;
import org.apache.commons.vfs2.provider.webdav4s.Webdav4sFileNameParser;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

/**
 * Parses {@code <connection-name>:///relative/path} into an internal {@code webdav4(s)} file name
 * by resolving the path against {@link WebDavConnection#getRootUrl()}.
 */
final class HopWebDavConnectionFileNameParser
    extends org.apache.commons.vfs2.provider.AbstractFileNameParser {

  private final IVariables variables;
  private final WebDavConnection meta;

  HopWebDavConnectionFileNameParser(IVariables variables, WebDavConnection meta) {
    this.variables = variables;
    this.meta = meta;
  }

  /**
   * Builds the internal {@code webdav4(s)} URI from a named-connection URI and resolved root URL.
   */
  static String buildSyntheticUri(String uri, String resolvedRootUrl) throws FileSystemException {
    String rootUrl = Const.NVL(resolvedRootUrl, "").trim();
    if (StringUtils.isEmpty(rootUrl)) {
      throw new FileSystemException("WebDAV connection has no rootUrl");
    }
    String lower = rootUrl.toLowerCase();
    if (!lower.startsWith("webdav4://") && !lower.startsWith("webdav4s://")) {
      throw new FileSystemException(
          "WebDAV rootUrl must start with webdav4:// or webdav4s://, got: " + rootUrl);
    }
    if (!rootUrl.endsWith("/")) {
      rootUrl = rootUrl + "/";
    }

    String suffix = HopWebDavLogicalUris.extractPathSuffixAfterScheme(uri);

    return suffix.isEmpty() ? rootUrl : rootUrl + suffix;
  }

  @Override
  public FileName parseUri(VfsComponentContext context, FileName base, String uri)
      throws FileSystemException {
    String rootUrl = Const.NVL(variables.resolve(meta.getRootUrl()), "").trim();
    if (StringUtils.isEmpty(rootUrl)) {
      throw new FileSystemException("WebDAV connection \"" + meta.getName() + "\" has no rootUrl");
    }
    String synthetic = buildSyntheticUri(uri, rootUrl);
    String lower = rootUrl.toLowerCase();
    FileNameParser delegate =
        lower.startsWith("webdav4s://")
            ? Webdav4sFileNameParser.getInstance()
            : Webdav4FileNameParser.getInstance();
    FileName parsed = delegate.parseUri(context, base, synthetic);
    if (!(parsed instanceof Webdav4FileName)) {
      return parsed;
    }
    Webdav4FileName wire = (Webdav4FileName) parsed;
    String rootPrefix = HopWebDavLogicalUris.wirePathPrefixFromRootUrl(rootUrl);
    String logicalRel = HopWebDavLogicalUris.extractPathSuffixAfterScheme(uri);
    return new HopWebDavConnectionFileName(wire, meta.getName(), logicalRel, rootPrefix);
  }
}
