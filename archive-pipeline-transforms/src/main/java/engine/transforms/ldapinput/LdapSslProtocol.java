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

package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.pipeline.transforms.ldapinput.store.CustomSocketFactory;

import java.util.Collection;
import java.util.Map;

public class LdapSslProtocol extends LdapProtocol {

  private final boolean trustAllCertificates;

  private final String trustStorePath;

  private final String trustStorePassword;

  public LdapSslProtocol( LogChannelInterface log, iVariables variables, LdapMeta meta,
                          Collection<String> binaryAttributes ) {
    super( log, variables, meta, binaryAttributes );
    String trustStorePath = null;
    String trustStorePassword = null;
    boolean trustAllCertificates = false;

    if ( meta.isUseCertificate() ) {
      trustStorePath = variables.environmentSubstitute( meta.getTrustStorePath() );
      trustStorePassword = Utils.resolvePassword( variables,
        meta.getTrustStorePassword() );
      trustAllCertificates = meta.isTrustAllCertificates();
    }

    this.trustAllCertificates = trustAllCertificates;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
  }

  @Override
  protected String getConnectionPrefix() {
    return "ldaps://";
  }

  public static String getName() {
    return "LDAP SSL";
  }

  protected void configureSslEnvironment( Map<String, String> env ) {
    env.put( javax.naming.Context.SECURITY_PROTOCOL, "ssl" );
    env.put( "java.naming.ldap.factory.socket", CustomSocketFactory.class.getCanonicalName() );
  }

  @Override
  protected void setupEnvironment( Map<String, String> env, String username, String password ) throws HopException {
    super.setupEnvironment( env, username, password );
    configureSslEnvironment( env );
    configureSocketFactory( trustAllCertificates, trustStorePath, trustStorePassword );
  }

  protected void configureSocketFactory( boolean trustAllCertificates, String trustStorePath,
                                         String trustStorePassword ) throws HopException {
    if ( trustAllCertificates ) {
      CustomSocketFactory.configure();
    } else {
      CustomSocketFactory.configure( trustStorePath, trustStorePassword );
    }
  }
}
