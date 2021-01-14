/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.plugins;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hop.core.Const;

import java.net.URL;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This describes the plugin itself, the IDs it listens too, what libraries (jar files) it uses, the names, the i18n
 * details, etc.
 *
 * @author matt
 */
public class Plugin implements IPlugin, Comparable<Plugin> {

  public static Comparator<IPlugin> nullStringComparator =
    ( p1, p2 ) -> new CompareToBuilder()
      .append( p1.getName(), p2.getName(), Comparator.nullsLast( String::compareToIgnoreCase ) )
      .append( p1.getIds(), p2.getIds() )
      .toComparison();

  private String category;
  private String name;
  private String description;
  private String[] ids;
  private Class<? extends IPluginType> pluginType;
  private String imageFile;
  private boolean separateClassLoaderNeeded;
  private String classLoaderGroup;
  private boolean nativePlugin;
  private Map<Class<?>, String> classMap;
  private List<String> libraries;
  private String errorHelpFile;
  private Class<?> mainType;
  private URL pluginFolder;
  private String documentationUrl;
  private String casesUrl;
  private String forumUrl;
  private String suggestion;
  private String[] keywords;
  private boolean usingLibrariesOutsidePluginFolder;

  public Plugin( String[] ids, Class<? extends IPluginType> pluginType, Class<?> mainType,
                 String category, String name, String description, String imageFile, boolean separateClassLoaderNeeded,
                 boolean nativePlugin, Map<Class<?>, String> classMap, List<String> libraries, String errorHelpFile, String[] keywords,
                 URL pluginFolder, boolean usingLibrariesOutsidePluginFolder ) {
    this( ids, pluginType, mainType, category, name, description, imageFile, separateClassLoaderNeeded,
      nativePlugin, classMap, libraries, errorHelpFile, keywords, pluginFolder, usingLibrariesOutsidePluginFolder, null, null, null );
  }

  public Plugin( String[] ids, Class<? extends IPluginType> pluginType, Class<?> mainType,
                 String category, String name, String description, String imageFile, boolean separateClassLoaderNeeded,
                 boolean nativePlugin, Map<Class<?>, String> classMap, List<String> libraries, String errorHelpFile, String[] keywords,
                 URL pluginFolder, boolean usingLibrariesOutsidePluginFolder, String documentationUrl, String casesUrl, String forumUrl ) {
    this( ids, pluginType, mainType, category, name, description, imageFile, separateClassLoaderNeeded, null,
      nativePlugin, classMap, libraries, errorHelpFile, keywords, pluginFolder, usingLibrariesOutsidePluginFolder, documentationUrl, casesUrl, forumUrl );
  }

  public Plugin( String[] ids, Class<? extends IPluginType> pluginType, Class<?> mainType,
                 String category, String name, String description, String imageFile, boolean separateClassLoaderNeeded,
                 String classLoaderGroup, boolean nativePlugin, Map<Class<?>, String> classMap, List<String> libraries,
                 String errorHelpFile, String[] keywords, URL pluginFolder, boolean usingLibrariesOutsidePluginFolder, String documentationUrl, String casesUrl, String forumUrl,
                 String suggestion ) {
    this( ids, pluginType, mainType, category, name, description, imageFile, separateClassLoaderNeeded,
      classLoaderGroup, nativePlugin, classMap, libraries, errorHelpFile, keywords, pluginFolder, usingLibrariesOutsidePluginFolder, documentationUrl, casesUrl,
      forumUrl );
    this.suggestion = suggestion;
  }

  public Plugin( String[] ids, Class<? extends IPluginType> pluginType, Class<?> mainType,
                 String category, String name, String description, String imageFile, boolean separateClassLoaderNeeded,
                 boolean nativePlugin, Map<Class<?>, String> classMap, List<String> libraries, String errorHelpFile, String[] keywords,
                 URL pluginFolder, boolean usingLibrariesOutsidePluginFolder, String documentationUrl, String casesUrl, String forumUrl, String suggestion ) {
    this( ids, pluginType, mainType, category, name, description, imageFile, separateClassLoaderNeeded, null,
      nativePlugin, classMap, libraries, errorHelpFile, keywords, pluginFolder, usingLibrariesOutsidePluginFolder, documentationUrl, casesUrl, forumUrl );
    this.suggestion = suggestion;
  }

  public Plugin( String[] ids, Class<? extends IPluginType> pluginType, Class<?> mainType,
                 String category, String name, String description, String imageFile, boolean separateClassLoaderNeeded,
                 String classLoaderGroup, boolean nativePlugin, Map<Class<?>, String> classMap, List<String> libraries,
                 String errorHelpFile, String[] keywords, URL pluginFolder, boolean usingLibrariesOutsidePluginFolder, String documentationUrl, String casesUrl, String forumUrl ) {
    this.ids = ids;
    this.pluginType = pluginType;
    this.mainType = mainType;
    this.category = category;
    this.name = name;
    this.description = description;
    this.imageFile = imageFile;
    this.separateClassLoaderNeeded = separateClassLoaderNeeded;
    this.classLoaderGroup = classLoaderGroup;
    this.nativePlugin = nativePlugin;
    this.classMap = classMap;
    this.libraries = libraries;
    this.errorHelpFile = errorHelpFile;
    this.keywords = keywords;
    this.pluginFolder = pluginFolder;
    this.usingLibrariesOutsidePluginFolder = usingLibrariesOutsidePluginFolder;
    this.documentationUrl = Const.getDocUrl( documentationUrl );
    this.casesUrl = casesUrl;
    this.forumUrl = forumUrl;
  }

  @Override
  public String toString() {
    return ids[ 0 ] + "/" + name + "{" + pluginType + "}";
  }

  @Override
  public boolean equals( Object obj ) {
    return ( obj != null ) && ( obj instanceof Plugin ) && compareTo( (Plugin) obj ) == 0;
  }

  @Override
  public int hashCode() {
    return ids[ 0 ].hashCode();
  }

  @Override
  public int compareTo( Plugin o ) {
    // All the IDs have to be the same to match, otherwise it's a different plugin
    // This might be a bit over the top, usually we only have a single ID
    //

    return nullStringComparator.compare( this, o );
  }

  @Override
  public boolean matches( String id ) {
    return Const.indexOfString( id, ids ) >= 0;
  }

  /**
   * @return the category
   */
  @Override
  public String getCategory() {
    return category;
  }

  /**
   * @param category the category to set
   */
  public void setCategory( String category ) {
    this.category = category;
  }

  /**
   * @return the name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the description
   */
  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * @return the ids
   */
  @Override
  public String[] getIds() {
    return ids;
  }

  /**
   * @param ids the ids to set
   */
  public void setIds( String[] ids ) {
    this.ids = ids;
  }

  /**
   * @return the pluginType
   */
  @Override
  public Class<? extends IPluginType> getPluginType() {
    return pluginType;
  }

  /**
   * @param pluginType the pluginType to set
   */
  public void setPluginType( Class<? extends IPluginType> pluginType ) {
    this.pluginType = pluginType;
  }

  /**
   * @return the imageFile
   */
  @Override
  public String getImageFile() {
    return imageFile;
  }

  /**
   * @param imageFile the imageFile to set
   */
  @Override
  public void setImageFile( String imageFile ) {
    this.imageFile = imageFile;
  }

  /**
   * @return the separateClassLoaderNeeded
   */
  @Override
  public boolean isSeparateClassLoaderNeeded() {
    return separateClassLoaderNeeded;
  }

  /**
   * @param separateClassLoaderNeeded the separateClassLoaderNeeded to set
   */
  public void setSaperateClassLoaderNeeded( boolean separateClassLoaderNeeded ) {
    this.separateClassLoaderNeeded = separateClassLoaderNeeded;
  }

  /**
   * @return the nativePlugin
   */
  @Override
  public boolean isNativePlugin() {
    return nativePlugin;
  }

  /**
   * @param nativePlugin the nativePlugin to set
   */
  public void setNativePlugin( boolean nativePlugin ) {
    this.nativePlugin = nativePlugin;
  }

  /**
   * @return the classMap
   */
  @Override
  public Map<Class<?>, String> getClassMap() {
    return classMap;
  }

  /**
   * @param classMap the classMap to set
   */
  public void setClassMap( Map<Class<?>, String> classMap ) {
    this.classMap = classMap;
  }

  /**
   * @return the libraries
   */
  @Override
  public List<String> getLibraries() {
    return libraries;
  }

  /**
   * @param libraries the libraries to set
   */
  public void setLibraries( List<String> libraries ) {
    this.libraries = libraries;
  }

  /**
   * @return the errorHelpFile
   */
  @Override
  public String getErrorHelpFile() {
    return errorHelpFile;
  }

  /**
   * @param errorHelpFile the errorHelpFile to set
   */
  @Override
  public void setErrorHelpFile( String errorHelpFile ) {
    this.errorHelpFile = errorHelpFile;
  }

  @Override
  public Class<?> getMainType() {
    return mainType;
  }

  @Override
  public URL getPluginDirectory() {
    return this.pluginFolder;
  }

  /**
   * @return the documentationUrl
   */
  @Override
  public String getDocumentationUrl() {
    return documentationUrl;
  }

  /**
   * @param documentationUrl the documentationUrl to set
   */
  @Override
  public void setDocumentationUrl( String documentationUrl ) {
    this.documentationUrl = documentationUrl;
  }

  /**
   * @return the casesUrl
   */
  @Override
  public String getCasesUrl() {
    return casesUrl;
  }

  /**
   * @param casesUrl the casesUrl to set
   */
  @Override
  public void setCasesUrl( String casesUrl ) {
    this.casesUrl = casesUrl;
  }

  /**
   * @return the forum URL
   */
  @Override
  public String getForumUrl() {
    return forumUrl;
  }

  /**
   * @param forumUrl the forum URL to set
   */
  @Override
  public void setForumUrl( String forumUrl ) {
    this.forumUrl = forumUrl;
  }

  @Override
  public String getClassLoaderGroup() {
    return classLoaderGroup;
  }

  @Override
  public void setSuggestion( String suggestion ) {
    this.suggestion = suggestion;
  }

  @Override
  public String getSuggestion() {
    return suggestion;
  }

  @Override
  public void setClassLoaderGroup( String classLoaderGroup ) {
    this.classLoaderGroup = classLoaderGroup;
  }

  /**
   * Gets keywords
   *
   * @return value of keywords
   */
  public String[] getKeywords() {
    return keywords;
  }

  /**
   * @param keywords The keywords to set
   */
  public void setKeywords( String[] keywords ) {
    this.keywords = keywords;
  }

  /**
   * Gets usingLibrariesOutsidePluginFolder
   *
   * @return value of usingLibrariesOutsidePluginFolder
   */
  @Override public boolean isUsingLibrariesOutsidePluginFolder() {
    return usingLibrariesOutsidePluginFolder;
  }

  /**
   * @param usingLibrariesOutsidePluginFolder The usingLibrariesOutsidePluginFolder to set
   */
  public void setUsingLibrariesOutsidePluginFolder( boolean usingLibrariesOutsidePluginFolder ) {
    this.usingLibrariesOutsidePluginFolder = usingLibrariesOutsidePluginFolder;
  }
}
