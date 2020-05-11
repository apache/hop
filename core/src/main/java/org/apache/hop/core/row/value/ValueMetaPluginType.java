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

package org.apache.hop.core.row.value;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.row.IValueMeta;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the value meta plugin type.
 *
 * @author matt
 */

@PluginMainClassType( IValueMeta.class )
@PluginAnnotationType( ValueMetaPlugin.class )
public class ValueMetaPluginType extends BasePluginType implements IPluginType {

  private static ValueMetaPluginType valueMetaPluginType;

  private ValueMetaPluginType() {
    super( ValueMetaPlugin.class, "VALUEMETA", "ValueMeta" );
    populateFolders( "valuemeta" );
  }

  public static ValueMetaPluginType getInstance() {
    if ( valueMetaPluginType == null ) {
      valueMetaPluginType = new ValueMetaPluginType();
    }
    return valueMetaPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_VALUEMETA_PLUGINS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_VALUEMETA_PLUGINS_FILE;
  }

  @Override public String getMainTag() {
    return "valuemeta-plugins";
  }

  @Override public String getSubTag() {
    return "valuemeta-plugin";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).isSeparateClassLoaderNeeded();
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
    return ( (ValueMetaPlugin) annotation ).documentationUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (ValueMetaPlugin) annotation ).classLoaderGroup();
  }
}
