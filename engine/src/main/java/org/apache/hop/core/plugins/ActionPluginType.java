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
import org.apache.hop.core.annotations.Action;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This plugin type handles the actions.
 *
 * @author matt
 */

@PluginTypeCategoriesOrder(
  getNaturalCategoriesOrder = {
    "ActionCategory.Category.General", "ActionCategory.Category.Mail", "ActionCategory.Category.FileManagement",
    "ActionCategory.Category.Conditions", "ActionCategory.Category.Scripting", "ActionCategory.Category.BulkLoading",
    "ActionCategory.Category.BigData", "ActionCategory.Category.Modeling", "ActionCategory.Category.DataQuality",
    "ActionCategory.Category.XML", "ActionCategory.Category.Utility", "ActionCategory.Category.Repository",
    "ActionCategory.Category.FileTransfer", "ActionCategory.Category.FileEncryption", "ActionCategory.Category.Palo",
    "ActionCategory.Category.Experimental", "ActionCategory.Category.Deprecated" }, i18nPackageClass = WorkflowMeta.class )
@PluginMainClassType( IAction.class )
@PluginAnnotationType( Action.class )
public class ActionPluginType extends BasePluginType implements IPluginType {
  private static Class<?> PKG = WorkflowMeta.class; // for i18n purposes, needed by Translator!!

  public static final String GENERAL_CATEGORY = BaseMessages.getString( PKG, "ActionCategory.Category.General" );

  private static ActionPluginType pluginType;

  private ActionPluginType() {
    super( Action.class, "ACTION", "Action" );
    populateFolders( "actions" );
  }

  protected ActionPluginType( Class<? extends Annotation> pluginType, String id, String name ) {
    super( pluginType, id, name );
  }

  public static ActionPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new ActionPluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_WORKFLOW_ACTIONS;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_CORE_WORKFLOW_ACTIONS_FILE;
  }

  @Override
  protected String getMainTag() {
    return "workflow-actions";
  }

  @Override
  protected String getSubTag() {
    return "workflow-action";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (Action) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (Action) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (Action) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (Action) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (Action) annotation ).image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (Action) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return Const.getDocUrl( ( (Action) annotation ).documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (Action) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (Action) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return ( (Action) annotation ).suggestion();
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (Action) annotation ).classLoaderGroup();
  }

  @Override protected String[] extractKeywords( Annotation annotation ) {
    return ( (Action) annotation ).keywords();
  }
}
