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

package org.apache.hop.core.auth.core;

import org.apache.hop.core.auth.DelegatingKerberosConsumer;
import org.apache.hop.core.auth.DelegatingKerberosConsumerForClassloaderBridging;
import org.apache.hop.core.auth.DelegatingNoAuthConsumer;
import org.apache.hop.core.auth.DelegatingUsernamePasswordConsumer;
import org.apache.hop.core.auth.KerberosAuthenticationProvider;
import org.apache.hop.core.auth.IKerberosAuthenticationProviderProxy;
import org.apache.hop.core.auth.NoAuthenticationAuthenticationProvider;
import org.apache.hop.core.auth.UsernamePasswordAuthenticationProvider;
import org.apache.hop.core.auth.core.impl.ClassloaderBridgingAuthenticationPerformer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticationManagerTest {
  private AuthenticationManager manager;
  private NoAuthenticationAuthenticationProvider noAuthenticationAuthenticationProvider;

  @Before
  public void setup() {
    manager = new AuthenticationManager();
    noAuthenticationAuthenticationProvider = new NoAuthenticationAuthenticationProvider();
    manager.registerAuthenticationProvider( noAuthenticationAuthenticationProvider );
  }

  @SuppressWarnings( "unchecked" )
  @Test
  public void testNoAuthProviderAndConsumer() throws AuthenticationConsumptionException, AuthenticationFactoryException {
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    IAuthenticationConsumer<Object, NoAuthenticationAuthenticationProvider> consumer =
      mock( IAuthenticationConsumer.class );
    manager.getAuthenticationPerformer( Object.class, IAuthenticationConsumer.class,
      NoAuthenticationAuthenticationProvider.NO_AUTH_ID ).perform( consumer );
    verify( consumer ).consume( noAuthenticationAuthenticationProvider );
  }

  @SuppressWarnings( "unchecked" )
  @Test
  public void testUsernamePasswordProviderConsumer() throws AuthenticationConsumptionException,
    AuthenticationFactoryException {
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    manager.registerConsumerClass( DelegatingUsernamePasswordConsumer.class );
    UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider =
      new UsernamePasswordAuthenticationProvider( "upass", "u", "pass" );
    manager.registerAuthenticationProvider( usernamePasswordAuthenticationProvider );
    IAuthenticationConsumer<Object, UsernamePasswordAuthenticationProvider> consumer =
      mock( IAuthenticationConsumer.class );
    manager.getAuthenticationPerformer( Object.class, IAuthenticationConsumer.class,
      usernamePasswordAuthenticationProvider.getId() ).perform( consumer );
    verify( consumer ).consume( usernamePasswordAuthenticationProvider );
  }

  @SuppressWarnings( "unchecked" )
  @Test
  public void testKerberosProviderConsumer() throws AuthenticationConsumptionException, AuthenticationFactoryException {
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    manager.registerConsumerClass( DelegatingUsernamePasswordConsumer.class );
    manager.registerConsumerClass( DelegatingKerberosConsumer.class );
    KerberosAuthenticationProvider kerberosAuthenticationProvider =
      new KerberosAuthenticationProvider( "kerb", "kerb", true, "pass", true, "none" );
    manager.registerAuthenticationProvider( kerberosAuthenticationProvider );
    IAuthenticationConsumer<Object, KerberosAuthenticationProvider> consumer = mock( IAuthenticationConsumer.class );
    manager.getAuthenticationPerformer( Object.class, IAuthenticationConsumer.class,
      kerberosAuthenticationProvider.getId() ).perform( consumer );
    verify( consumer ).consume( kerberosAuthenticationProvider );
  }

  @SuppressWarnings( "rawtypes" )
  @Test
  public void testGetSupportedPerformers() throws AuthenticationConsumptionException, AuthenticationFactoryException {
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    manager.registerConsumerClass( DelegatingUsernamePasswordConsumer.class );
    manager.registerConsumerClass( DelegatingKerberosConsumer.class );
    UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider =
      new UsernamePasswordAuthenticationProvider( "upass", "u", "pass" );
    manager.registerAuthenticationProvider( usernamePasswordAuthenticationProvider );
    KerberosAuthenticationProvider kerberosAuthenticationProvider =
      new KerberosAuthenticationProvider( "kerb", "kerb", true, "pass", true, "none" );
    manager.registerAuthenticationProvider( kerberosAuthenticationProvider );
    List<IAuthenticationPerformer<Object, IAuthenticationConsumer>> performers =
      manager.getSupportedAuthenticationPerformers( Object.class, IAuthenticationConsumer.class );
    assertEquals( 3, performers.size() );
    Set<String> ids =
      new HashSet<>( Arrays.asList( NoAuthenticationAuthenticationProvider.NO_AUTH_ID,
        usernamePasswordAuthenticationProvider.getId(), kerberosAuthenticationProvider.getId() ) );
    for ( IAuthenticationPerformer<Object, IAuthenticationConsumer> performer : performers ) {
      ids.remove( performer.getAuthenticationProvider().getId() );
    }
    assertEquals( 0, ids.size() );
  }

  @SuppressWarnings( "rawtypes" )
  @Test
  public void testRegisterUnregisterProvider() throws AuthenticationFactoryException {
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    manager.registerConsumerClass( DelegatingUsernamePasswordConsumer.class );
    List<IAuthenticationPerformer<Object, IAuthenticationConsumer>> performers =
      manager.getSupportedAuthenticationPerformers( Object.class, IAuthenticationConsumer.class );
    assertEquals( 1, performers.size() );
    Set<String> ids = new HashSet<>( Arrays.asList( NoAuthenticationAuthenticationProvider.NO_AUTH_ID ) );
    for ( IAuthenticationPerformer<Object, IAuthenticationConsumer> performer : performers ) {
      ids.remove( performer.getAuthenticationProvider().getId() );
    }
    assertEquals( 0, ids.size() );
    UsernamePasswordAuthenticationProvider usernamePasswordAuthenticationProvider =
      new UsernamePasswordAuthenticationProvider( "upass", "u", "pass" );
    manager.registerAuthenticationProvider( usernamePasswordAuthenticationProvider );
    performers = manager.getSupportedAuthenticationPerformers( Object.class, IAuthenticationConsumer.class );
    assertEquals( 2, performers.size() );
    ids =
      new HashSet<>( Arrays.asList( NoAuthenticationAuthenticationProvider.NO_AUTH_ID,
        usernamePasswordAuthenticationProvider.getId() ) );
    for ( IAuthenticationPerformer<Object, IAuthenticationConsumer> performer : performers ) {
      ids.remove( performer.getAuthenticationProvider().getId() );
    }
    assertEquals( 0, ids.size() );
    manager.unregisterAuthenticationProvider( usernamePasswordAuthenticationProvider );
    performers = manager.getSupportedAuthenticationPerformers( Object.class, IAuthenticationConsumer.class );
    assertEquals( 1, performers.size() );
    ids = new HashSet<>( Arrays.asList( NoAuthenticationAuthenticationProvider.NO_AUTH_ID ) );
    for ( IAuthenticationPerformer<Object, IAuthenticationConsumer> performer : performers ) {
      ids.remove( performer.getAuthenticationProvider().getId() );
    }
    assertEquals( 0, ids.size() );
  }

  @SuppressWarnings( { "rawtypes", "unchecked" } )
  @Test
  public void testRegisterConsumerFactory() throws AuthenticationConsumptionException, AuthenticationFactoryException {
    IAuthenticationConsumer<Object, KerberosAuthenticationProvider> authConsumer = mock( IAuthenticationConsumer.class );
    IAuthenticationConsumerFactory<Object, IAuthenticationConsumer, KerberosAuthenticationProvider> factory =
      mock( IAuthenticationConsumerFactory.class );
    when( factory.getReturnType() ).thenReturn( Object.class );
    when( factory.getCreateArgType() ).thenReturn( IAuthenticationConsumer.class );
    when( factory.getConsumedType() ).thenReturn( KerberosAuthenticationProvider.class );
    when( factory.create( authConsumer ) ).thenReturn( authConsumer );
    KerberosAuthenticationProvider kerberosAuthenticationProvider =
      new KerberosAuthenticationProvider( "kerb", "kerb", true, "pass", true, "none" );
    manager.registerAuthenticationProvider( kerberosAuthenticationProvider );
    manager.registerConsumerFactory( factory );
    manager.getAuthenticationPerformer( Object.class, IAuthenticationConsumer.class,
      kerberosAuthenticationProvider.getId() ).perform( authConsumer );
    verify( authConsumer ).consume( kerberosAuthenticationProvider );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testClassLoaderBridgingPerformer() throws AuthenticationConsumptionException,
    AuthenticationFactoryException {
    manager.setAuthenticationPerformerFactory( new IAuthenticationPerformerFactory() {

      @Override
      public <ReturnType, CreateArgType, ConsumedType> IAuthenticationPerformer<ReturnType, CreateArgType> create(
        IAuthenticationProvider authenticationProvider,
        IAuthenticationConsumerFactory<ReturnType, CreateArgType, ConsumedType> authenticationConsumer ) {
        if ( AuthenticationConsumerInvocationHandler.isCompatible( authenticationConsumer.getConsumedType(),
          authenticationProvider ) ) {
          return new ClassloaderBridgingAuthenticationPerformer<>(
            authenticationProvider, authenticationConsumer );
        }
        return null;
      }
    } );
    manager.registerConsumerClass( DelegatingNoAuthConsumer.class );
    manager.registerConsumerClass( DelegatingUsernamePasswordConsumer.class );
    manager.registerConsumerClass( DelegatingKerberosConsumerForClassloaderBridging.class );
    KerberosAuthenticationProvider kerberosAuthenticationProvider =
      new KerberosAuthenticationProvider( "kerb", "kerb", true, "pass", true, "none" );
    manager.registerAuthenticationProvider( kerberosAuthenticationProvider );
    IAuthenticationConsumer<Object, IKerberosAuthenticationProviderProxy> consumer =
      mock( IAuthenticationConsumer.class );

    @SuppressWarnings( "rawtypes" )
    IAuthenticationPerformer<Object, IAuthenticationConsumer> performer =
      manager.getAuthenticationPerformer( Object.class, IAuthenticationConsumer.class, kerberosAuthenticationProvider
        .getId() );
    assertNotNull( performer );
    performer.perform( consumer );

    ArgumentCaptor<IKerberosAuthenticationProviderProxy> captor =
      ArgumentCaptor.forClass( IKerberosAuthenticationProviderProxy.class );
    verify( consumer ).consume( captor.capture() );
    assertEquals( kerberosAuthenticationProvider.getId(), captor.getValue().getId() );
    assertEquals( kerberosAuthenticationProvider.getDisplayName(), captor.getValue().getDisplayName() );
    assertEquals( kerberosAuthenticationProvider.getPrincipal(), captor.getValue().getPrincipal() );
    assertEquals( kerberosAuthenticationProvider.getPassword(), captor.getValue().getPassword() );
    assertEquals( kerberosAuthenticationProvider.getKeytabLocation(), captor.getValue().getKeytabLocation() );
    assertEquals( kerberosAuthenticationProvider.isUseKeytab(), captor.getValue().isUseKeytab() );
    assertEquals( kerberosAuthenticationProvider.isUseExternalCredentials(), captor.getValue()
      .isUseExternalCredentials() );
  }
}
