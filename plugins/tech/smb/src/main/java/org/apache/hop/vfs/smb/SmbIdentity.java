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
package org.apache.hop.vfs.smb;

/**
 * Domain and username passed to NTLM. A filled domain field wins. Otherwise {@code DOMAIN\\user} in
 * the username is split. {@code user@dns.domain} is left as the username: smbj wants the NetBIOS
 * domain in its own field.
 */
public record SmbIdentity(String domain, String username) {

  public static SmbIdentity parse(String domainField, String usernameField) {
    String domain = trim(domainField);
    String username = trim(usernameField);
    int slash = username.indexOf('\\');
    if (slash >= 0) {
      if (domain.isEmpty()) {
        domain = username.substring(0, slash).trim();
      }
      username = username.substring(slash + 1).trim();
    }
    return new SmbIdentity(domain, username);
  }

  private static String trim(String value) {
    return value == null ? "" : value.trim();
  }
}
