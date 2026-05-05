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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileNameParser;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystem;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.webdav4s.Webdav4sFileNameParser;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

/**
 * Named WebDAV connection: logical {@link HopWebDavFileName} with scheme = metadata name; wire I/O
 * via an internal {@link Webdav4FileSystem} (same idea as other Hop cloud VFS types using {@link
 * AbstractOriginatingFileProvider} + dedicated file names).
 */
public class HopWebDavFileProvider extends AbstractOriginatingFileProvider {

  private static final Collection<Capability> CAPABILITIES;

  static {
    Collection<Capability> c = new ArrayList<>(new Webdav4FileProvider().getCapabilities());
    CAPABILITIES = Collections.unmodifiableCollection(c);
  }

  private final IVariables variables;
  private final WebDavConnection meta;

  public HopWebDavFileProvider(IVariables variables, WebDavConnection meta) {
    this.variables = variables;
    this.meta = meta;
    setFileNameParser(new HopWebDavFileNameParser(meta));
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return CAPABILITIES;
  }

  @Override
  public org.apache.commons.vfs2.FileSystemConfigBuilder getConfigBuilder() {
    return Webdav4FileSystemConfigBuilder.getInstance();
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    FileSystemOptions opts =
        fileSystemOptions != null
            ? (FileSystemOptions) fileSystemOptions.clone()
            : new FileSystemOptions();
    HopWebDavConnectionAuth.apply(opts, variables, meta);

    String rootUrl = Const.NVL(variables.resolve(meta.getRootUrl()), "").trim();
    if (StringUtils.isEmpty(rootUrl)) {
      throw new FileSystemException("WebDAV connection \"" + meta.getName() + "\" has no rootUrl");
    }
    String lower = rootUrl.toLowerCase();
    if (!lower.startsWith("webdav4://") && !lower.startsWith("webdav4s://")) {
      throw new FileSystemException(
          "WebDAV rootUrl must start with webdav4:// or webdav4s://, got: " + rootUrl);
    }
    FileNameParser wireParser =
        lower.startsWith("webdav4s://")
            ? Webdav4sFileNameParser.getInstance()
            : Webdav4FileNameParser.getInstance();
    FileName wireRootParsed = wireParser.parseUri(getContext(), null, rootUrl);
    Webdav4FileName wireRoot = HopWebDavWireNames.asWebdav4(wireRootParsed);

    Webdav4FileSystem wireFs = HopWebDavWireBootstrap.create(opts, wireRoot);
    wireFs.init();

    String prefix = HopWebDavLogicalUris.wirePathPrefixFromRootUrl(rootUrl);
    return new HopWebDavFileSystem((HopWebDavFileName) rootName, opts, wireFs, wireRoot, prefix);
  }
}
