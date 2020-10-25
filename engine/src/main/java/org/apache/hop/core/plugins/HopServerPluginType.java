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

package org.apache.hop.core.plugins;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.www.IHopServerPlugin;

import java.util.Map;

/**
 * This class represents the carte plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IHopServerPlugin.class )
@PluginAnnotationType( HopServerServlet.class )
public class HopServerPluginType extends BasePluginType<HopServerServlet> implements IPluginType<HopServerServlet> {

  private static HopServerPluginType hopServerPluginType;

  private HopServerPluginType() {
    super( HopServerServlet.class, "HOP_SERVER_SERVLET", "HopServer Servlet" );
    populateFolders( "servlets" );
  }

  public static HopServerPluginType getInstance() {
    if ( hopServerPluginType == null ) {
      hopServerPluginType = new HopServerPluginType();
    }
    return hopServerPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_SERVLETS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_CORE_SERVLETS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "servlets";
  }

  @Override
  protected String getSubTag() {
    return "servlet";
  }

  @Override
  protected String extractCategory( HopServerServlet annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( HopServerServlet annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractID( HopServerServlet annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( HopServerServlet annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile( HopServerServlet annotation ) {
    return "";
  }

  @Override
  protected boolean extractSeparateClassLoader( HopServerServlet annotation ) {
    return annotation.isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( HopServerServlet annotation ) {
    return annotation.i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, HopServerServlet annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( HopServerServlet annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( HopServerServlet annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( HopServerServlet annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( HopServerServlet annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( HopServerServlet annotation ) {
    return annotation.classLoaderGroup();
  }
}
