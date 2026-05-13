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
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.http4.Http4FileSystemConfigBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.webdav.metadata.WebDavConnection;

final class HopWebDavConnectionAuth {

  private HopWebDavConnectionAuth() {}

  static void apply(FileSystemOptions opts, IVariables variables, WebDavConnection meta) {
    String user = Const.NVL(variables.resolve(meta.getUsername()), "");
    String pass = Const.NVL(Utils.resolvePassword(variables, meta.getPassword()), "");
    if (StringUtils.isNotEmpty(user)) {
      DefaultFileSystemConfigBuilder.getInstance()
          .setUserAuthenticator(opts, new StaticUserAuthenticator(null, user, pass));
    }
    Http4FileSystemConfigBuilder http4 = Http4FileSystemConfigBuilder.getInstance();
    http4.setFollowRedirect(opts, meta.isFollowRedirects());
    http4.setPreemptiveAuth(opts, meta.isPreemptiveAuth());
  }
}
