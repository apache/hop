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
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.www.IHopServerPlugin;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the carte plugin type.
 *
 * @author matt
 */
@PluginMainClassType( IHopServerPlugin.class )
@PluginAnnotationType( HopServerServlet.class )
public class HopServerPluginType extends BasePluginType implements IPluginType {

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
