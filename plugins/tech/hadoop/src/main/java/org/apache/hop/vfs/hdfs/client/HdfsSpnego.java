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
package org.apache.hop.vfs.hdfs.client;

import java.util.Base64;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Builds a {@code Authorization: Negotiate} header with the JDK GSS API. Must run inside {@code
 * Subject.doAs} so the TGT on the subject is used. HttpClient 5's SPNEGO scheme is deprecated; this
 * keeps Hadoop client JARs out of the plugin.
 */
public final class HdfsSpnego {
  private static final Oid SPNEGO = oid("1.3.6.1.5.5.2");

  private HdfsSpnego() {}

  public static String authorizationHeader(String host) throws GSSException {
    GSSManager manager = GSSManager.getInstance();
    GSSName serverName = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
    GSSContext context =
        manager.createContext(serverName, SPNEGO, null, GSSContext.DEFAULT_LIFETIME);
    try {
      context.requestMutualAuth(true);
      context.requestCredDeleg(false);
      byte[] token = context.initSecContext(new byte[0], 0, 0);
      if (token == null || token.length == 0) {
        throw new GSSException(GSSException.NO_CRED);
      }
      return "Negotiate " + Base64.getEncoder().encodeToString(token);
    } finally {
      context.dispose();
    }
  }

  private static Oid oid(String str) {
    try {
      return new Oid(str);
    } catch (GSSException e) {
      throw new IllegalStateException(e);
    }
  }
}
