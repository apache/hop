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
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.IJobEntry;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 * This plugin type handles the job entries.
 *
 * @author matt
 */

@PluginTypeCategoriesOrder(
  getNaturalCategoriesOrder = {
    "JobCategory.Category.General", "JobCategory.Category.Mail", "JobCategory.Category.FileManagement",
    "JobCategory.Category.Conditions", "JobCategory.Category.Scripting", "JobCategory.Category.BulkLoading",
    "JobCategory.Category.BigData", "JobCategory.Category.Modeling", "JobCategory.Category.DataQuality",
    "JobCategory.Category.XML", "JobCategory.Category.Utility", "JobCategory.Category.Repository",
    "JobCategory.Category.FileTransfer", "JobCategory.Category.FileEncryption", "JobCategory.Category.Palo",
    "JobCategory.Category.Experimental", "JobCategory.Category.Deprecated" }, i18nPackageClass = JobMeta.class )
@PluginMainClassType( IJobEntry.class )
@PluginAnnotationType( JobEntry.class )
public class JobEntryPluginType extends BasePluginType implements IPluginType {
  private static Class<?> PKG = JobMeta.class; // for i18n purposes, needed by Translator!!

  public static final String GENERAL_CATEGORY = BaseMessages.getString( PKG, "JobCategory.Category.General" );

  private static JobEntryPluginType pluginType;

  private JobEntryPluginType() {
    super( JobEntry.class, "JOBENTRY", "Job entry" );
    populateFolders( "jobentries" );
  }

  protected JobEntryPluginType( Class<? extends Annotation> pluginType, String id, String name ) {
    super( pluginType, id, name );
  }

  public static JobEntryPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new JobEntryPluginType();
    }
    return pluginType;
  }

  @Override
  protected String getXmlPluginFile() {
    return Const.XML_FILE_HOP_JOB_ENTRIES;
  }

  @Override
  protected String getAlternativePluginFile() {
    return Const.HOP_CORE_JOBENTRIES_FILE;
  }

  @Override
  protected String getMainTag() {
    return "job-entries";
  }

  @Override
  protected String getSubTag() {
    return "job-entry";
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (JobEntry) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (JobEntry) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (JobEntry) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (JobEntry) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (JobEntry) annotation ).image();
  }

  @Override
  protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (JobEntry) annotation ).i18nPackageName();
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return Const.getDocUrl( ( (JobEntry) annotation ).documentationUrl() );
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (JobEntry) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (JobEntry) annotation ).forumUrl();
  }

  @Override
  protected String extractSuggestion( Annotation annotation ) {
    return ( (JobEntry) annotation ).suggestion();
  }

  @Override
  protected String extractClassLoaderGroup( Annotation annotation ) {
    return ( (JobEntry) annotation ).classLoaderGroup();
  }

  @Override protected String[] extractKeywords( Annotation annotation ) {
    return ( (JobEntry) annotation ).keywords();
  }
}
