/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.steps.ldapinput;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;

import org.junit.Before;
import org.junit.Test;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.trans.steps.ldapinput.store.CustomSocketFactory;

public class LdapTlsProtocolIT {
  private LogChannelInterface mockLogChannelInterface;

  private VariableSpace mockVariableSpace;

  private LdapMeta mockLdapMeta;

  private InitialLdapContext mockInitialLdapContext;

  private StartTlsResponse mockStartTlsResponse;

  private class TestableLdapTlsProtocol extends LdapTlsProtocol {
    public Hashtable<String, String> contextEnv = null;

    public TestableLdapTlsProtocol( LogChannelInterface log, VariableSpace variableSpace, LdapMeta meta,
      Collection<String> binaryAttributes ) {
      super( log, variableSpace, meta, binaryAttributes );
    }

    @Override
    protected InitialLdapContext createLdapContext( Hashtable<String, String> env ) throws NamingException {
      contextEnv = env;
      return mockInitialLdapContext;
    }

    @Override
    protected void configureSocketFactory( boolean trustAllCertificates, String trustStorePath,
      String trustStorePassword ) throws HopException {
      CustomSocketFactory.configure();
    }
  }

  @Before
  public void setup() throws NamingException {
    mockLogChannelInterface = mock( LogChannelInterface.class );
    mockVariableSpace = mock( VariableSpace.class );
    mockLdapMeta = mock( LdapMeta.class );
    mockInitialLdapContext = mock( InitialLdapContext.class );
    mockStartTlsResponse = mock( StartTlsResponse.class );
    when( mockInitialLdapContext.extendedOperation( any( StartTlsRequest.class ) ) ).thenReturn(
      mockStartTlsResponse );
  }

  @Test
  public void testLdapProtocolAddsLdapPrefixIfNecessary() throws HopException, NamingException {
    String hostConcrete = "host_concrete";
    String portConcrete = "12345";
    when( mockLdapMeta.getHost() ).thenReturn( hostConcrete );
    when( mockLdapMeta.getPort() ).thenReturn( portConcrete );
    when( mockLdapMeta.getDerefAliases() ).thenReturn( "always" );
    when( mockLdapMeta.getReferrals() ).thenReturn( "follow" );

    when( mockVariableSpace.environmentSubstitute( eq( hostConcrete ) ) ).thenReturn( hostConcrete );
    when( mockVariableSpace.environmentSubstitute( eq( portConcrete ) ) ).thenReturn( portConcrete );

    TestableLdapTlsProtocol testableLdapProtocol =
      new TestableLdapTlsProtocol( mockLogChannelInterface, mockVariableSpace, mockLdapMeta, null );
    testableLdapProtocol.connect( null, null );

    assertEquals(
      testableLdapProtocol.getConnectionPrefix() + hostConcrete + ":" + portConcrete,
      testableLdapProtocol.contextEnv.get( Context.PROVIDER_URL ) );
  }

  @Test
  public void testLdapProtocolSkipsAddingLdapPrefixIfNecessary() throws HopException {
    String hostnameConcrete = "host_concrete";
    String hostConcrete = "ldap://" + hostnameConcrete;
    String portConcrete = "12345";
    when( mockLdapMeta.getHost() ).thenReturn( hostConcrete );
    when( mockLdapMeta.getPort() ).thenReturn( portConcrete );
    when( mockLdapMeta.getDerefAliases() ).thenReturn( "always" );
    when( mockLdapMeta.getReferrals() ).thenReturn( "follow" );

    when( mockVariableSpace.environmentSubstitute( eq( hostConcrete ) ) ).thenReturn( hostConcrete );
    when( mockVariableSpace.environmentSubstitute( eq( portConcrete ) ) ).thenReturn( portConcrete );

    TestableLdapTlsProtocol testableLdapProtocol =
      new TestableLdapTlsProtocol( mockLogChannelInterface, mockVariableSpace, mockLdapMeta, null );
    testableLdapProtocol.connect( null, null );

    assertEquals(
      testableLdapProtocol.getConnectionPrefix() + hostnameConcrete + ":" + portConcrete,
      testableLdapProtocol.contextEnv.get( Context.PROVIDER_URL ) );
  }

  @Test
  public void testLdapProtocolSetsSsl() throws HopException {
    String hostConcrete = "host_concrete";
    String portConcrete = "12345";
    when( mockLdapMeta.getHost() ).thenReturn( hostConcrete );
    when( mockLdapMeta.getPort() ).thenReturn( portConcrete );
    when( mockLdapMeta.getDerefAliases() ).thenReturn( "always" );
    when( mockLdapMeta.getReferrals() ).thenReturn( "follow" );

    when( mockVariableSpace.environmentSubstitute( eq( hostConcrete ) ) ).thenReturn( hostConcrete );
    when( mockVariableSpace.environmentSubstitute( eq( portConcrete ) ) ).thenReturn( portConcrete );

    TestableLdapTlsProtocol testableLdapProtocol =
      new TestableLdapTlsProtocol( mockLogChannelInterface, mockVariableSpace, mockLdapMeta, null );
    testableLdapProtocol.connect( null, null );

    assertEquals( null, testableLdapProtocol.contextEnv.get( Context.SECURITY_PROTOCOL ) );
  }

  @Test
  public void testLdapProtocolSetsSocketFactory() throws HopException {
    String hostConcrete = "host_concrete";
    String portConcrete = "12345";
    when( mockLdapMeta.getHost() ).thenReturn( hostConcrete );
    when( mockLdapMeta.getPort() ).thenReturn( portConcrete );
    when( mockLdapMeta.getDerefAliases() ).thenReturn( "always" );
    when( mockLdapMeta.getReferrals() ).thenReturn( "follow" );

    when( mockVariableSpace.environmentSubstitute( eq( hostConcrete ) ) ).thenReturn( hostConcrete );
    when( mockVariableSpace.environmentSubstitute( eq( portConcrete ) ) ).thenReturn( portConcrete );

    TestableLdapTlsProtocol testableLdapProtocol =
      new TestableLdapTlsProtocol( mockLogChannelInterface, mockVariableSpace, mockLdapMeta, null );
    testableLdapProtocol.connect( null, null );

    assertEquals( null, testableLdapProtocol.contextEnv.get( "java.naming.ldap.factory.socket" ) );
  }

  @Test
  public void testLdapProtocolNegotiatesTls() throws HopException, IOException {
    String hostConcrete = "host_concrete";
    String portConcrete = "12345";
    when( mockLdapMeta.getHost() ).thenReturn( hostConcrete );
    when( mockLdapMeta.getPort() ).thenReturn( portConcrete );
    when( mockLdapMeta.getDerefAliases() ).thenReturn( "always" );
    when( mockLdapMeta.getReferrals() ).thenReturn( "follow" );

    when( mockVariableSpace.environmentSubstitute( eq( hostConcrete ) ) ).thenReturn( hostConcrete );
    when( mockVariableSpace.environmentSubstitute( eq( portConcrete ) ) ).thenReturn( portConcrete );

    TestableLdapTlsProtocol testableLdapProtocol =
      new TestableLdapTlsProtocol( mockLogChannelInterface, mockVariableSpace, mockLdapMeta, null );
    testableLdapProtocol.connect( null, null );

    verify( mockStartTlsResponse ).negotiate( any( CustomSocketFactory.class ) );
  }
}
