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
 * How this connection signs in. {@code toString()} stays {@link #name()} because the metadata combo
 * stores that text.
 *
 * <p>Kerberos is the next value to add here: a new constant, a new {@link SmbAuthenticator}, and
 * keytab fields on the same {@code smb-connection} metadata type. It is not implemented yet.
 */
public enum SmbAuthType {
  /** NTLMv2 with a username, a password, and an optional domain. */
  NTLM,
  /**
   * Guest. Current Windows has guest SMB2 access off by default. Kept so a lab share that still
   * allows it can be opened, and so a second authenticator exists behind the factory.
   */
  GUEST
}
