/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.variables.IVariables;
import org.mockito.Mockito;

import javax.naming.ldap.InitialLdapContext;
import java.util.Collection;

/**
 * Mock LDAP connection protocol, for testing
 *
 * @author nhudak
 */
public class LdapMockProtocol extends LdapProtocol {
  public static InitialLdapContext mockContext;

  public LdapMockProtocol( LogChannelInterface log, IVariables variables, LdapMeta meta,
                           Collection<String> binaryAttributes ) {
    super( log, variables, meta, binaryAttributes );
  }

  public static String getName() {
    return "LDAP MOCK";
  }

  public static InitialLdapContext setup() {
    LdapProtocolFactory.protocols.add( LdapMockProtocol.class );
    return mockContext = Mockito.mock( InitialLdapContext.class );
  }

  public static void cleanup() {
    LdapProtocolFactory.protocols.remove( LdapMockProtocol.class );
    mockContext = null;
  }

  @Override
  protected void doConnect( String username, String password ) throws HopException {
    if ( mockContext == null ) {
      throw new RuntimeException( "LDAP Mock Connection was not setup" );
    }
  }

  @Override
  public InitialLdapContext getCtx() {
    if ( mockContext == null ) {
      throw new RuntimeException( "LDAP Mock Connection was not setup" );
    } else {
      return mockContext;
    }
  }

  @Override
  public void close() throws HopException {
    mockContext = null;
  }
}
