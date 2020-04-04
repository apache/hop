/*! ******************************************************************************
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

package org.apache.hop.core.encryption;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.IPluginType;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the value meta plugin type.
 *
 * @author matt
 */

@PluginMainClassType( ITwoWayPasswordEncoder.class )
@PluginAnnotationType( TwoWayPasswordEncoderPlugin.class )
public class TwoWayPasswordEncoderPluginType extends BasePluginType implements IPluginType {

  private static TwoWayPasswordEncoderPluginType twoWayPasswordEncoderPluginType;

  private TwoWayPasswordEncoderPluginType() {
    super( TwoWayPasswordEncoderPlugin.class, "TWOWAYPASSWORDENCODERPLUGIN", "TwoWayPasswordEncoder" );
    populateFolders( "passwordencoder" );
  }

  public static TwoWayPasswordEncoderPluginType getInstance() {
    if ( twoWayPasswordEncoderPluginType == null ) {
      twoWayPasswordEncoderPluginType = new TwoWayPasswordEncoderPluginType();
    }
    return twoWayPasswordEncoderPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_PASSWORD_ENCODER_PLUGINS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_PASSWORD_ENCODER_PLUGINS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "password-encoder-plugins";
  }

  @Override
  protected String getSubTag() {
    return "password-encoder-plugin";
  }

  @Override
  protected boolean isReturn() {
    return true;
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (TwoWayPasswordEncoderPlugin) annotation ).classLoaderGroup();
  }
}
