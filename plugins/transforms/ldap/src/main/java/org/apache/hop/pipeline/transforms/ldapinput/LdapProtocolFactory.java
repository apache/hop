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
package org.apache.hop.pipeline.transforms.ldapinput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

public class LdapProtocolFactory {
  protected static final List<Class<? extends LdapProtocol>> protocols = initProtocols();

  private static List<Class<? extends LdapProtocol>> initProtocols() {
    List<Class<? extends LdapProtocol>> protocols = new ArrayList<>();
    protocols.add(LdapProtocol.class);
    protocols.add(LdapSslProtocol.class);
    protocols.add(LdapTlsProtocol.class);
    return protocols;
  }

  private final ILogChannel log;

  private static String getName(Class<? extends LdapProtocol> protocol) throws HopException {
    try {
      return protocol.getMethod("getName").invoke(null).toString();
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Returns the connection types understood by the factory
   *
   * @return the connection types understood by the factory
   * @throws HopException
   */
  public static final List<String> getConnectionTypes(ILogChannel log) {
    List<String> result = new ArrayList<>();
    synchronized (protocols) {
      for (Class<? extends LdapProtocol> protocol : protocols) {
        try {
          result.add(getName(protocol));
        } catch (HopException e) {
          log.logError("Unable to get name for " + protocol.getCanonicalName());
        }
      }
    }
    return result;
  }

  public LdapProtocolFactory(ILogChannel log) {
    this.log = log;
  }

  /**
   * Creates the LdapProtocol appropriate for the ILdapMeta
   *
   * @param variables the variable variables for environment substitutions
   * @param meta the ldap meta
   * @param binaryAttributes binary attributes to associate with the connection
   * @return an LdapProtocol
   * @throws HopException
   */
  public LdapProtocol createLdapProtocol(
      IVariables variables, ILdapMeta meta, Collection<String> binaryAttributes)
      throws HopException {
    String connectionType = variables.resolve(meta.getProtocol());

    synchronized (protocols) {
      for (Class<? extends LdapProtocol> protocol : protocols) {
        if (getName(protocol).equals(connectionType)) {
          try {
            return protocol
                .getConstructor(
                    ILogChannel.class, IVariables.class, ILdapMeta.class, Collection.class)
                .newInstance(log, variables, meta, binaryAttributes);
          } catch (Exception e) {
            throw new HopException(e);
          }
        }
      }
    }
    return null;
  }
}
