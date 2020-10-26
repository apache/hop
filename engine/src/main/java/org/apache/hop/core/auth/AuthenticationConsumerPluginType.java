/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the authentication plugin type.
 */
@PluginMainClassType( IAuthenticationConsumerType.class )
@PluginAnnotationType( AuthenticationConsumerPlugin.class )
public class AuthenticationConsumerPluginType extends BasePluginType<AuthenticationConsumerPlugin> implements IPluginType<AuthenticationConsumerPlugin> {
  protected static AuthenticationConsumerPluginType pluginType = new AuthenticationConsumerPluginType();

  private AuthenticationConsumerPluginType() {
    super( AuthenticationConsumerPlugin.class, "AUTHENTICATION_CONSUMER", "IAuthenticationConsumer" );
    populateFolders( "authentication" );
  }

  public static AuthenticationConsumerPluginType getInstance() {
    return pluginType;
  }

  @Override
  protected String extractCategory( AuthenticationConsumerPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).description();
  }

  @Override
  protected String extractID( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).id();
  }

  @Override
  protected String extractName( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).name();
  }

  @Override
  protected boolean extractSeparateClassLoader( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, AuthenticationConsumerPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( AuthenticationConsumerPlugin annotation ) {
    return ( (AuthenticationConsumerPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( AuthenticationConsumerPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractImageFile( AuthenticationConsumerPlugin annotation ) {
    return "";
  }

  @Override
  protected void registerNatives() throws HopPluginException {

  }
}
