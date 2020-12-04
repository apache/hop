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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

import java.util.Map;

/**
 * This plugin allows you to capture additional information concerning pipelines
 *
 * @author matt
 */
@PluginMainClassType( IPipelineDialogPlugin.class )
@PluginAnnotationType( PipelineDialogPlugin.class )
public class PipelineDialogPluginType extends BasePluginType<PipelineDialogPlugin> implements IPluginType<PipelineDialogPlugin> {

  private static PipelineDialogPluginType pluginType;

  private PipelineDialogPluginType() {
    super( PipelineDialogPlugin.class, "PIPELINE_DIALOG", "Pipeline dialog" );
    populateFolders( "pipelinedialog" );
  }

  public static PipelineDialogPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new PipelineDialogPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( PipelineDialogPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( PipelineDialogPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( PipelineDialogPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( PipelineDialogPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( PipelineDialogPlugin annotation ) {
    return annotation.i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, PipelineDialogPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( PipelineDialogPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( PipelineDialogPlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
