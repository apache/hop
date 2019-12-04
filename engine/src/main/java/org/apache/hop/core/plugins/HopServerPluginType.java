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

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.www.HopServerPluginInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * This class represents the carte plugin type.
 *
 * @author matt
 *
 */
@PluginMainClassType( HopServerPluginInterface.class )
@PluginAnnotationType( HopServerServlet.class )
public class HopServerPluginType extends BasePluginType implements PluginTypeInterface {

  private static HopServerPluginType hopServerPluginType;

  private HopServerPluginType() {
    super( HopServerServlet.class, "CARTE_SERVLET", "HopServer Servlet" );
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

  protected void registerXmlPlugins() throws HopPluginException {
    for ( PluginFolderInterface folder : pluginFolders ) {

      if ( folder.isPluginXmlFolder() ) {
        List<FileObject> pluginXmlFiles = findPluginXmlFiles( folder.getFolder() );
        for ( FileObject file : pluginXmlFiles ) {

          try {
            Document document = XMLHandler.loadXMLFile( file );
            Node pluginNode = XMLHandler.getSubNode( document, "plugin" );
            if ( pluginNode != null ) {
              registerPluginFromXmlResource( pluginNode, HopVFS.getFilename( file.getParent() ), this
                .getClass(), false, file.getParent().getURL() );
            }
          } catch ( Exception e ) {
            // We want to report this plugin.xml error, perhaps an XML typo or
            // something like that...
            //
            log.logError( "Error found while reading step plugin.xml file: " + file.getName().toString(), e );
          }
        }
      }
    }
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return "";
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (HopServerServlet) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (HopServerServlet) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (HopServerServlet) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return "";
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (HopServerServlet) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (HopServerServlet) annotation ).i18nPackageName();
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
    return ( (HopServerServlet) annotation ).classLoaderGroup();
  }
}
