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

package org.apache.hop.core.database;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** How Hop signs in to one SSH server of a tunnel: with a password or with a private key. */
public enum SshAuthenticationMethod implements IEnumHasCodeAndDescription {
  /** Password authentication, answered for both password and keyboard-interactive prompts. */
  PASSWORD,

  /** Public key authentication with a private key file and an optional passphrase. */
  PRIVATE_KEY;

  private static final Class<?> PKG = SshAuthenticationMethod.class;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "SshAuthenticationMethod." + name() + ".Description");
  }

  /**
   * Finds a method by its code, its description or its constant name, ignoring case.
   *
   * @param value the stored code or the description shown in a combo
   * @return the matching method, or {@link #PASSWORD} when nothing matches
   */
  public static SshAuthenticationMethod lookup(String value) {
    if (value == null) {
      return PASSWORD;
    }
    for (SshAuthenticationMethod method : values()) {
      if (method.getCode().equalsIgnoreCase(value)
          || method.getDescription().equalsIgnoreCase(value)) {
        return method;
      }
    }
    return PASSWORD;
  }
}
