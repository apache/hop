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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

/**
 * This class represents the step plugin type.
 *
 * @author matt
 */
@PluginTypeCategoriesOrder(
  getNaturalCategoriesOrder = {
    "BaseStep.Category.Input",
    "BaseStep.Category.Output",
    "BaseStep.Category.Streaming",
    "BaseStep.Category.Transform",
    "BaseStep.Category.Utility",
    "BaseStep.Category.Flow",
    "BaseStep.Category.Scripting",
    "BaseStep.Category.BAServer",
    "BaseStep.Category.Lookup",
    "BaseStep.Category.Joins",
    "BaseStep.Category.DataWarehouse",
    "BaseStep.Category.Validation",
    "BaseStep.Category.Statistics",
    "BaseStep.Category.DataMining",
    "BaseStep.Category.BigData",
    "BaseStep.Category.Agile",
    "BaseStep.Category.DataQuality",
    "BaseStep.Category.Cryptography",
    "BaseStep.Category.Palo",
    "BaseStep.Category.OpenERP",
    "BaseStep.Category.Job",
    "BaseStep.Category.Mapping",
    "BaseStep.Category.Bulk",
    "BaseStep.Category.Inline",
    "BaseStep.Category.Experimental",
    "BaseStep.Category.Deprecated" },
  i18nPackageClass = StepInterface.class )
@PluginMainClassType( StepMetaInterface.class )
@PluginAnnotationType( Step.class )
public class StepPluginType extends BasePluginType implements PluginTypeInterface {

  private static StepPluginType stepPluginType;

  protected StepPluginType() {
    super( Step.class, "STEP", "Step" );
    populateFolders( "steps" );
  }

  public static StepPluginType getInstance() {
    if ( stepPluginType == null ) {
      stepPluginType = new StepPluginType();
    }
    return stepPluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_STEPS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_CORE_STEPS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "steps";
  }

  @Override
  protected String getSubTag() {
    return "step";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (Step) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (Step) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (Step) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (Step) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (Step) annotation ).image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return ( (Step) annotation ).isSeparateClassLoaderNeeded();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (Step) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return Const.getDocUrl( ( (Step) annotation ).documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (Step) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (Step) annotation ).forumUrl();
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (Step) annotation ).classLoaderGroup();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return ( (Step) annotation ).suggestion();
  }

  @Override protected String[] extractKeywords( Annotation annotation ) {
    return ( (Step)annotation).keywords();
  }
}
