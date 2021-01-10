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

import org.apache.hop.core.auth.core.impl.DefaultAuthenticationConsumerFactory;
import org.apache.hop.core.auth.core.impl.DefaultAuthenticationPerformerFactory;
import org.apache.hop.i18n.BaseMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AuthenticationManager {
  private static final Class<?> PKG = AuthenticationManager.class; // For Translator
  private final Map<Class<?>, Map<Class<?>, Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>>>>
      factoryMap = new HashMap<>();
  private IAuthenticationPerformerFactory authenticationPerformerFactory =
      new DefaultAuthenticationPerformerFactory();
  private final List<IAuthenticationProvider> authenticationProviders = new ArrayList<>();

  public void registerAuthenticationProvider(IAuthenticationProvider authenticationProvider) {
    synchronized (authenticationProviders) {
      authenticationProviders.add(authenticationProvider);
    }
  }

  public boolean unregisterAuthenticationProvider(IAuthenticationProvider authenticationProvider) {
    synchronized (authenticationProviders) {
      return authenticationProviders.remove(authenticationProvider);
    }
  }

  public <ReturnType, CreateArgType, ConsumedType> void registerConsumerFactory(
      IAuthenticationConsumerFactory<ReturnType, CreateArgType, ConsumedType> factory)
      throws AuthenticationFactoryException {
    if (!factory.getConsumedType().isInterface()
        && !IAuthenticationProvider.class.isAssignableFrom(factory.getConsumedType())) {
      throw new AuthenticationFactoryException(
          BaseMessages.getString(PKG, "AuthenticationManager.ConsumedTypeError", factory));
    }

    Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>> createTypeMap =
        getRelevantConsumerFactoryMap(factory.getReturnType(), factory.getCreateArgType());
    synchronized (createTypeMap) {
      createTypeMap.put(factory.getConsumedType(), factory);
    }
  }

  public <ReturnType, ConsumedType> void registerConsumerClass(
      Class<? extends IAuthenticationConsumer<? extends ReturnType, ? extends ConsumedType>>
          consumerClass)
      throws AuthenticationFactoryException {
    registerConsumerFactory(new DefaultAuthenticationConsumerFactory(consumerClass));
  }

  public <ReturnType, CreateArgType, ConsumedType>
      List<IAuthenticationPerformer<ReturnType, CreateArgType>>
          getSupportedAuthenticationPerformers(
              Class<ReturnType> returnType, Class<CreateArgType> createArgType) {
    Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>> createTypeMap =
        getRelevantConsumerFactoryMap(returnType, createArgType);
    synchronized (createTypeMap) {
      createTypeMap = new HashMap<>(createTypeMap);
    }

    List<IAuthenticationProvider> authenticationProviders;
    synchronized (this.authenticationProviders) {
      authenticationProviders = new ArrayList<>(this.authenticationProviders);
    }

    List<IAuthenticationPerformer<ReturnType, CreateArgType>> result = new ArrayList<>();

    for (Entry<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>> entry :
        createTypeMap.entrySet()) {
      for (IAuthenticationProvider provider : authenticationProviders) {
        @SuppressWarnings("unchecked")
        IAuthenticationPerformer<ReturnType, CreateArgType> authenticationPerformer =
            (IAuthenticationPerformer<ReturnType, CreateArgType>)
                authenticationPerformerFactory.create(provider, entry.getValue());
        if (authenticationPerformer != null && authenticationPerformer.getDisplayName() != null) {
          result.add(authenticationPerformer);
        }
      }
    }

    Collections.sort(
        result,
        (o1, o2) -> o1.getDisplayName().toUpperCase().compareTo(o2.getDisplayName().toUpperCase()));

    return result;
  }

  public <ReturnType, CreateArgType, ConsumedType>
      IAuthenticationPerformer<ReturnType, CreateArgType> getAuthenticationPerformer(
          Class<ReturnType> returnType, Class<CreateArgType> createArgType, String providerId) {
    List<IAuthenticationPerformer<ReturnType, CreateArgType>> performers =
        getSupportedAuthenticationPerformers(returnType, createArgType);
    for (IAuthenticationPerformer<ReturnType, CreateArgType> candidatePerformer : performers) {
      if (candidatePerformer.getAuthenticationProvider().getId().equals(providerId)) {
        return candidatePerformer;
      }
    }
    return null;
  }

  private <ReturnType, CreateArgType>
      Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>> getRelevantConsumerFactoryMap(
          Class<ReturnType> returnType, Class<CreateArgType> createArgType) {
    synchronized (factoryMap) {
      Map<Class<?>, Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>>> returnTypeMap =
          factoryMap.get(returnType);
      if (returnTypeMap == null) {
        returnTypeMap = new HashMap<>();
        factoryMap.put(returnType, returnTypeMap);
      }

      Map<Class<?>, IAuthenticationConsumerFactory<?, ?, ?>> createTypeMap =
          returnTypeMap.get(createArgType);
      if (createTypeMap == null) {
        createTypeMap = new HashMap<>();
        returnTypeMap.put(createArgType, createTypeMap);
      }

      return createTypeMap;
    }
  }

  protected void setAuthenticationPerformerFactory(
      IAuthenticationPerformerFactory authenticationPerformerFactory) {
    this.authenticationPerformerFactory = authenticationPerformerFactory;
  }
}
