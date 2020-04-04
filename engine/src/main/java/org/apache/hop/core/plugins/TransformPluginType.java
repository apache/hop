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
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This class represents the transform plugin type.
 *
 * @author matt
 */
@PluginTypeCategoriesOrder(
  getNaturalCategoriesOrder = {
    "BaseTransform.Category.Input",
    "BaseTransform.Category.Output",
    "BaseTransform.Category.Streaming",
    "BaseTransform.Category.Transform",
    "BaseTransform.Category.Utility",
    "BaseTransform.Category.Flow",
    "BaseTransform.Category.Scripting",
    "BaseTransform.Category.Lookup",
    "BaseTransform.Category.Joins",
    "BaseTransform.Category.DataWarehouse",
    "BaseTransform.Category.Validation",
    "BaseTransform.Category.Statistics",
    "BaseTransform.Category.DataMining",
    "BaseTransform.Category.BigData",
    "BaseTransform.Category.Agile",
    "BaseTransform.Category.DataQuality",
    "BaseTransform.Category.Cryptography",
    "BaseTransform.Category.Palo",
    "BaseTransform.Category.OpenERP",
    "BaseTransform.Category.Job",
    "BaseTransform.Category.Mapping",
    "BaseTransform.Category.Bulk",
    "BaseTransform.Category.Inline",
    "BaseTransform.Category.Experimental",
    "BaseTransform.Category.Deprecated" },
  i18nPackageClass = ITransform.class )
@PluginMainClassType( ITransformMeta.class )
@PluginAnnotationType( Transform.class )
public class TransformPluginType extends BasePluginType implements IPluginType {

  private static TransformPluginType transformPluginType;

  protected TransformPluginType() {
    super( Transform.class, "TRANSFORM", "Transform" );
    populateFolders( "transforms" );
  }

  public static TransformPluginType getInstance() {
    if ( transformPluginType == null ) {
      transformPluginType = new TransformPluginType();
    }
    return transformPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_TRANSFORMS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_CORE_TRANSFORMS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "transforms";
  }

  @Override
  protected String getSubTag() {
    return "transform";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (Transform) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (Transform) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (Transform) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (Transform) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (Transform) annotation ).image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (Transform) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (Transform) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return Const.getDocUrl( ( (Transform) annotation ).documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (Transform) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (Transform) annotation ).forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (Transform) annotation ).classLoaderGroup();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return ( (Transform) annotation ).suggestion();
  }

  @Override protected String[] extractKeywords( Annotation annotation ) {
    return ( (Transform) annotation ).keywords();
  }
}
