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

package org.apache.hop.core.plugins;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PartitionerPlugin;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.pipeline.IPartitioner;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This is the partitioner plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IPartitioner.class )
@PluginAnnotationType( PartitionerPlugin.class )
public class PartitionerPluginType extends BasePluginType implements IPluginType {

  private static PartitionerPluginType pluginType;

  private PartitionerPluginType() {
    super( PartitionerPlugin.class, "PARTITIONER", "IPartitioner" );
    populateFolders( "transforms" );
  }

  public static PartitionerPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new PartitionerPluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_PARTITION_PLUGINS;
  }

  @Override
  protected String getMainTag() {
    return "plugins";
  }

  @Override
  protected String getSubTag() {
    return "plugin-partitioner";
  }

  /**
   * Scan & register internal transform plugins
   */
  protected void registerAnnotations() throws HopPluginException {
    // This is no longer done because it was deemed too slow. Only jar files in the plugins/ folders are scanned for
    // annotations.
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (PartitionerPlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (PartitionerPlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (PartitionerPlugin) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (PartitionerPlugin) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (PartitionerPlugin) annotation ).classLoaderGroup();
  }
}
