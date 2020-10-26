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

package org.apache.hop.core.plugins;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopLifecyclePlugin;
import org.apache.hop.core.lifecycle.IHopLifecycleListener;

import java.util.Map;

/**
 * Defines a Hop Environment lifecycle plugin type. These plugins are invoked at Hop Environment initialization
 * and shutdown.
 */
@PluginMainClassType( IHopLifecycleListener.class )
@PluginAnnotationType( HopLifecyclePlugin.class )
public class HopLifecyclePluginType extends BasePluginType<HopLifecyclePlugin> implements IPluginType<HopLifecyclePlugin> {

  private static HopLifecyclePluginType pluginType;

  private HopLifecyclePluginType() {
    super( HopLifecyclePlugin.class, "HOP LIFECYCLE LISTENERS", "Hop Lifecycle Listener Plugin Type" );
    // We must call populate folders so PluginRegistry will look in the correct
    // locations for plugins (jars with annotations)
    populateFolders( null );
  }

  public static synchronized HopLifecyclePluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new HopLifecyclePluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_LIFECYCLE_LISTENERS;
  }

  @Override
  protected String getMainTag() {
    return "listeners";
  }

  @Override
  protected String getSubTag() {
    return "listener";
  }

  @Override
  protected boolean isReturn() {
    return true;
  }

  @Override
  protected String extractID( HopLifecyclePlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( HopLifecyclePlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractDesc( HopLifecyclePlugin annotation ) {
    return "";
  }

  @Override
  protected String extractCategory( HopLifecyclePlugin annotation ) {
    // No images, not shown in UI
    return "";
  }

  @Override
  protected String extractImageFile( HopLifecyclePlugin annotation ) {
    // No images, not shown in UI
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( HopLifecyclePlugin annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( HopLifecyclePlugin annotation ) {
    // No UI, no i18n
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, HopLifecyclePlugin annotation ) {
    classMap.put( HopLifecyclePlugin.class, clazz.getName() );
  }

  @Override
  protected String extractDocumentationUrl( HopLifecyclePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( HopLifecyclePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( HopLifecyclePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( HopLifecyclePlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( HopLifecyclePlugin annotation ) {
    return annotation.classLoaderGroup();
  }
}
