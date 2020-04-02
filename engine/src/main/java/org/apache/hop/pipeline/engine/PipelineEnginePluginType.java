/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.engine;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginFolder;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.IPluginType;

import java.lang.annotation.Annotation;
import java.util.Map;

@PluginMainClassType( IPipelineEngine.class )
@PluginAnnotationType( PipelineEnginePlugin.class )
public class PipelineEnginePluginType extends BasePluginType implements IPluginType {

  private PipelineEnginePluginType() {
    super( PipelineEnginePlugin.class, "HOP_PIPELINE_ENGINES", "Hop Pipeline Engines" );

    pluginFolders.add( new PluginFolder( "plugins", false, true ) );
  }

  private static PipelineEnginePluginType pluginType;

  public static PipelineEnginePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new PipelineEnginePluginType();
    }
    return pluginType;
  }

  @Override
  protected void registerNatives() throws HopPluginException {
    super.registerNatives();
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_PIPELINE_ENGINES;
  }

  @Override
  protected String getMainTag() {
    return "hop-engines";
  }

  @Override
  protected String getSubTag() {
    return "hop-engine";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (PipelineEnginePlugin) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (PipelineEnginePlugin) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (PipelineEnginePlugin) annotation ).name();
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
    return null;
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
    return null;
  }
}
