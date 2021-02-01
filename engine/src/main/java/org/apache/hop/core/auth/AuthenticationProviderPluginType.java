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

package org.apache.hop.core.auth;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This class represents the authentication plugin type.
 */
@PluginMainClassType( IAuthenticationProviderType.class )
@PluginAnnotationType( AuthenticationProviderPlugin.class )
public class AuthenticationProviderPluginType extends BasePluginType<AuthenticationProviderPlugin> {
  protected static AuthenticationProviderPluginType pluginType = new AuthenticationProviderPluginType();

  private AuthenticationProviderPluginType() {
    super( AuthenticationProviderPlugin.class, "AUTHENTICATION_PROVIDER", "Authentication Provider" );
  }

  public static AuthenticationProviderPluginType getInstance() {
    return pluginType;
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( AuthenticationProviderPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).description();
  }

  @Override
  protected String extractID( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).id();
  }

  @Override
  protected String extractName( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).name();
  }

  @Override
  protected boolean extractSeparateClassLoader( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, AuthenticationProviderPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( AuthenticationProviderPlugin annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( AuthenticationProviderPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractImageFile( AuthenticationProviderPlugin annotation ) {
    return "";
  }
}
