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

package org.apache.hop.core.auth.core.impl;

import org.apache.hop.core.auth.core.IAuthenticationConsumerFactory;
import org.apache.hop.core.auth.core.AuthenticationConsumerInvocationHandler;
import org.apache.hop.core.auth.core.IAuthenticationPerformer;
import org.apache.hop.core.auth.core.IAuthenticationPerformerFactory;
import org.apache.hop.core.auth.core.IAuthenticationProvider;

public class DefaultAuthenticationPerformerFactory implements IAuthenticationPerformerFactory {

  @SuppressWarnings( { "rawtypes", "unchecked" } )
  @Override
  public <ReturnType, CreateArgType, ConsumedType> IAuthenticationPerformer<ReturnType, CreateArgType> create(
    IAuthenticationProvider authenticationProvider,
    IAuthenticationConsumerFactory<ReturnType, CreateArgType, ConsumedType> authenticationConsumerFactory ) {
    if ( authenticationConsumerFactory.getConsumedType().isInstance( authenticationProvider ) ) {
      return new DefaultAuthenticationPerformer( authenticationProvider, authenticationConsumerFactory );
    } else if ( AuthenticationConsumerInvocationHandler.isCompatible( authenticationConsumerFactory.getConsumedType(),
      authenticationProvider ) ) {
      return new ClassloaderBridgingAuthenticationPerformer<ReturnType, CreateArgType, ConsumedType>(
        authenticationProvider, authenticationConsumerFactory );
    }
    return null;
  }
}
