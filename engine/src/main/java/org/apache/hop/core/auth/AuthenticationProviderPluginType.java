/*******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the authentication plugin type.
 */
@PluginMainClassType( IAuthenticationProviderType.class )
@PluginAnnotationType( AuthenticationProviderPlugin.class )
public class AuthenticationProviderPluginType extends BasePluginType implements IPluginType {
  protected static AuthenticationProviderPluginType pluginType = new AuthenticationProviderPluginType();

  private AuthenticationProviderPluginType() {
    super( AuthenticationProviderPlugin.class, "AUTHENTICATION_PROVIDER", "IAuthenticationProvider" );
    populateFolders( "authentication" );
  }

  public static AuthenticationProviderPluginType getInstance() {
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_AUTHENTICATION_PROVIDERS;
  }

  @Override
  protected String getMainTag() {
    return "authentication-providers";
  }

  @Override
  protected String getSubTag() {
    return "authentication-provider";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).name();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (AuthenticationProviderPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return "";
  }
}
