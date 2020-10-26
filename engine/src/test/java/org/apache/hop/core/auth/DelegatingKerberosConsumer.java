/*******************************************************************************
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

package org.apache.hop.core.auth;

import org.apache.hop.core.auth.core.IAuthenticationConsumer;
import org.apache.hop.core.auth.core.AuthenticationConsumptionException;

public class DelegatingKerberosConsumer implements
  IAuthenticationConsumer<Object, KerberosAuthenticationProvider> {
  private IAuthenticationConsumer<Object, KerberosAuthenticationProvider> delegate;

  public DelegatingKerberosConsumer( IAuthenticationConsumer<Object, KerberosAuthenticationProvider> delegate ) {
    this.delegate = delegate;
  }

  @Override
  public Object consume( KerberosAuthenticationProvider authenticationProvider ) throws AuthenticationConsumptionException {
    return delegate.consume( authenticationProvider );
  }
}
