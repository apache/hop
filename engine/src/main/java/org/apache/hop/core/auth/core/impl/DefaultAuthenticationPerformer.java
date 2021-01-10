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
import org.apache.hop.core.auth.core.AuthenticationConsumptionException;
import org.apache.hop.core.auth.core.IAuthenticationPerformer;
import org.apache.hop.core.auth.core.IAuthenticationProvider;

public class DefaultAuthenticationPerformer<ReturnType, CreateArgType, T extends IAuthenticationProvider> implements
  IAuthenticationPerformer<ReturnType, CreateArgType> {
  private final T provider;
  private final IAuthenticationConsumerFactory<ReturnType, CreateArgType, T> authenticationConsumerFactory;

  public DefaultAuthenticationPerformer( T provider,
                                         IAuthenticationConsumerFactory<ReturnType, CreateArgType, T> authenticationConsumerFactory ) {
    this.provider = provider;
    this.authenticationConsumerFactory = authenticationConsumerFactory;
  }

  @Override
  public ReturnType perform( CreateArgType consumerCreateArg ) throws AuthenticationConsumptionException {
    IAuthenticationConsumer<ReturnType, T> consumer = authenticationConsumerFactory.create( consumerCreateArg );
    return consumer.consume( provider );
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
