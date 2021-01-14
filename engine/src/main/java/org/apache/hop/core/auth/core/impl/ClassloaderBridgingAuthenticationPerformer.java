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

package org.apache.hop.core.auth.core.impl;

import org.apache.hop.core.auth.core.IAuthenticationConsumer;
import org.apache.hop.core.auth.core.IAuthenticationConsumerFactory;
import org.apache.hop.core.auth.core.AuthenticationConsumerInvocationHandler;
import org.apache.hop.core.auth.core.AuthenticationConsumptionException;
import org.apache.hop.core.auth.core.IAuthenticationPerformer;
import org.apache.hop.core.auth.core.IAuthenticationProvider;

import java.lang.reflect.Proxy;

public class ClassloaderBridgingAuthenticationPerformer<ReturnType, CreateArgType, ConsumedType> implements
  IAuthenticationPerformer<ReturnType, CreateArgType> {
  private final IAuthenticationProvider provider;
  private final IAuthenticationConsumerFactory<ReturnType, CreateArgType, ConsumedType> authenticationConsumerFactory;

  public ClassloaderBridgingAuthenticationPerformer( IAuthenticationProvider provider,
                                                     IAuthenticationConsumerFactory<ReturnType, CreateArgType, ConsumedType> authenticationConsumerFactory ) {
    this.provider = provider;
    this.authenticationConsumerFactory = authenticationConsumerFactory;
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public ReturnType perform( CreateArgType consumerCreateArg ) throws AuthenticationConsumptionException {
    IAuthenticationConsumer<ReturnType, ConsumedType> consumer =
      authenticationConsumerFactory.create( consumerCreateArg );
    ConsumedType providerProxy =
      (ConsumedType) Proxy.newProxyInstance( consumer.getClass().getClassLoader(),
        new Class[] { authenticationConsumerFactory.getConsumedType() },
        new AuthenticationConsumerInvocationHandler( provider ) );
    return consumer.consume( providerProxy );
  }

  @Override
  public String getDisplayName() {
    return provider.getDisplayName();
  }

  @Override
  public IAuthenticationProvider getAuthenticationProvider() {
    return provider;
  }
}
