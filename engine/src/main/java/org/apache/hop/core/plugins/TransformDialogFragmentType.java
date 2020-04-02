/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.pipeline.transform.TransformDialogInterface;

import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.List;

/**
 * This class represents the transform dialog fragment type.
 */
@PluginMainClassType( TransformDialogInterface.class )
@PluginAnnotationType( PluginDialog.class )
public class TransformDialogFragmentType extends BaseFragmentType implements PluginTypeInterface {

  private static TransformDialogFragmentType transformDialogFragmentType;

  protected TransformDialogFragmentType() {
    super( PluginDialog.class, "TRANSFORM_DIALOG", "Plugin Transform Dialog", TransformPluginType.class );
  }

  public static TransformDialogFragmentType getInstance() {
    if ( transformDialogFragmentType == null ) {
      transformDialogFragmentType = new TransformDialogFragmentType();
    }
    return transformDialogFragmentType;
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (PluginDialog) annotation ).id();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (PluginDialog) annotation ).image();
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return Const.getDocUrl( ( (PluginDialog) annotation ).documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (PluginDialog) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (PluginDialog) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override
  public void handlePluginAnnotation( Class<?> clazz, java.lang.annotation.Annotation annotation,
                                      List<String> libraries, boolean nativePluginType, URL pluginFolder ) throws HopPluginException {
    if ( ( (PluginDialog) annotation ).pluginType() == PluginDialog.PluginType.TRANSFORM ) {
      super.handlePluginAnnotation( clazz, annotation, libraries, nativePluginType, pluginFolder );
    }
  }
}
