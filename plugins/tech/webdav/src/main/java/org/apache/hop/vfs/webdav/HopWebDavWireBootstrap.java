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

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileProvider;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystem;
import org.apache.commons.vfs2.provider.webdav4.Webdav4FileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.webdav4s.Webdav4sFileProvider;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;

/** Builds an internal {@link Webdav4FileSystem} using stock WebDAV4 HTTP(S) client setup. */
final class HopWebDavWireBootstrap {

  private HopWebDavWireBootstrap() {}

  static Webdav4FileSystem create(FileSystemOptions opts, GenericFileName wireRoot)
      throws FileSystemException {
    Webdav4FileSystemConfigBuilder builder = Webdav4FileSystemConfigBuilder.getInstance();
    UserAuthenticationData auth = null;
    try {
      auth = UserAuthenticatorUtils.authenticate(opts, Webdav4FileProvider.AUTHENTICATOR_TYPES);
      if (wireRoot.getScheme().equalsIgnoreCase("webdav4s")) {
        return HttpsWire.create(wireRoot, opts, builder, auth);
      }
      return HttpWire.create(wireRoot, opts, builder, auth);
    } finally {
      UserAuthenticatorUtils.cleanup(auth);
    }
  }

  /** Subclass to access {@link Webdav4FileProvider} protected HTTP factory methods. */
  private static final class HttpWire extends Webdav4FileProvider {
    static Webdav4FileSystem create(
        GenericFileName wireRoot,
        FileSystemOptions opts,
        Webdav4FileSystemConfigBuilder builder,
        UserAuthenticationData auth)
        throws FileSystemException {
      HttpWire p = new HttpWire();
      HttpClientContext ctx = p.createHttpClientContext(builder, wireRoot, opts, auth);
      HttpClient client = p.createHttpClient(builder, wireRoot, opts);
      return new HopRawWebDavFileSystem(wireRoot, opts, client, ctx);
    }
  }

  private static final class HttpsWire extends Webdav4sFileProvider {
    static Webdav4FileSystem create(
        GenericFileName wireRoot,
        FileSystemOptions opts,
        Webdav4FileSystemConfigBuilder builder,
        UserAuthenticationData auth)
        throws FileSystemException {
      HttpsWire p = new HttpsWire();
      HttpClientContext ctx = p.createHttpClientContext(builder, wireRoot, opts, auth);
      HttpClient client = p.createHttpClient(builder, wireRoot, opts);
      return new HopRawWebDavFileSystem(wireRoot, opts, client, ctx);
    }
  }
}
