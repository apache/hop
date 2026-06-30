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

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This interface describes the plugin itself, the IDs it listens too, what libraries (jar files) it
 * uses, the names, the i18n detailes, etc.
 */
public interface IPlugin {

  /**
   * @return All the possible IDs that this plugin corresponds with.<br>
   *     Multiple IDs are typically used when you migrate 2 different plugins into a single one with
   *     the same functionality.<br>
   *     It can also happen if you deprecate an older plugin and you want to have a new one provide
   *     compatibility for it.<br>
   */
  String[] getIds();

  /**
   * @return The type of plugin
   */
  Class<? extends IPluginType> getPluginType();

  /**
   * @return The main class assigned to this Plugin.
   */
  Class<?> getMainType();

  /**
   * @return The libraries (jar file names) that are used by this plugin
   */
  List<String> getLibraries();

  /**
   * @return The name of the plugin
   */
  String getName();

  /**
   * @return The description of the plugin
   */
  String getDescription();

  /**
   * @return The location of the image (icon) file for this plugin
   */
  String getImageFile();

  /**
   * @param imageFile the location of the image (icon) file for this plugin
   */
  void setImageFile(String imageFile);

  /**
   * @return The category of this plugin or null if this is not applicable
   */
  String getCategory();

  /**
   * @return True if a separate class loader is needed every time this class is instantiated
   */
  boolean isSeparateClassLoaderNeeded();

  /**
   * @return true if this is considered to be a standard native plugin.
   */
  boolean isNativePlugin();

  /**
   * @return All the possible class names that can be loaded with this plugin, split up by type.
   */
  Map<Class<?>, String> getClassMap();

  /**
   * @param id the plugin id to match
   * @return true if one of the ids matches the given argument. Return false if it doesn't.
   */
  boolean matches(String id);

  /**
   * @return An optional location to a help file that the plugin can refer to in case there is a
   *     loading problem. This usually happens if a jar file is not installed correctly (class not
   *     found exceptions) etc.
   */
  String getErrorHelpFile();

  /**
   * @param errorHelpFile the errorHelpFile to set
   */
  void setErrorHelpFile(String errorHelpFile);

  /**
   * @return keywords describing this plugin
   */
  String[] getKeywords();

  /**
   * @param keywords keywords describing this plugin
   */
  void setKeywords(String[] keywords);

  URL getPluginDirectory();

  /**
   * @return the documentationUrl
   */
  String getDocumentationUrl();

  /**
   * @param documentationUrl the documentationUrl to set
   */
  void setDocumentationUrl(String documentationUrl);

  /**
   * @return The cases URL of the plugin
   */
  String getCasesUrl();

  /**
   * @param casesUrl the cases URL to set for this plugin
   */
  void setCasesUrl(String casesUrl);

  /**
   * @return the forum URL
   */
  String getForumUrl();

  /**
   * @param forumUrl the forum URL to set
   */
  void setForumUrl(String forumUrl);

  /**
   * @return The group to which this class loader belongs. Returns null if the plugin does not
   *     belong to a group (the default)
   */
  String getClassLoaderGroup();

  void setSuggestion(String suggestion);

  String getSuggestion();

  /**
   * @param group The group to which this class loader belongs. Set to null if the plugin does not
   *     belong to a group (the default)
   */
  void setClassLoaderGroup(String group);

  /**
   * @param fragment A plugin interface to merge with
   */
  default void merge(IPlugin fragment) {
    if (fragment != null) {
      Optional.ofNullable(fragment.getClassMap()).ifPresent(this.getClassMap()::putAll);
      Optional.ofNullable(fragment.getImageFile()).ifPresent(this::setImageFile);
      Optional.ofNullable(fragment.getLibraries()).ifPresent(this.getLibraries()::addAll);
      Optional.ofNullable(fragment.getErrorHelpFile()).ifPresent(this::setErrorHelpFile);
      Optional.ofNullable(fragment.getDocumentationUrl()).ifPresent(this::setDocumentationUrl);
      Optional.ofNullable(fragment.getCasesUrl()).ifPresent(this::setCasesUrl);
      Optional.ofNullable(fragment.getForumUrl()).ifPresent(this::setForumUrl);
      Optional.ofNullable(fragment.getClassLoaderGroup()).ifPresent(this::setClassLoaderGroup);
      String[] fragmentSupported = fragment.getSupportedEngines();
      if (fragmentSupported != null && fragmentSupported.length > 0) {
        this.setSupportedEngines(
            EngineCompatibilityResolver.unionEngineIds(
                this.getSupportedEngines(), fragmentSupported));
      }
      String[] fragmentExcluded = fragment.getExcludedEngines();
      if (fragmentExcluded != null && fragmentExcluded.length > 0) {
        this.setExcludedEngines(
            EngineCompatibilityResolver.unionEngineIds(
                this.getExcludedEngines(), fragmentExcluded));
      }
    }
  }

  /**
   * Engine plugin id patterns this transform/action is supported on. Default is empty (no opinion).
   * Trailing wildcard supported: {@code "Beam*"}.
   *
   * <p>Default returns an empty array so existing external {@link IPlugin} implementations stay
   * binary-compatible. Concrete plugins backed by {@code @Transform}/{@code @Action} annotations
   * populate this from the annotation; compatibility fragments add to it via {@link #merge}.
   */
  default String[] getSupportedEngines() {
    return new String[0];
  }

  /**
   * Setter counterpart to {@link #getSupportedEngines()}. Default no-op so existing {@link IPlugin}
   * implementations stay binary-compatible — concrete plugins ({@link Plugin}) override this to
   * actually store the value. Only used by {@link #merge}.
   */
  default void setSupportedEngines(String[] supportedEngines) {
    // intentional no-op default: external IPlugin implementers don't have to participate
  }

  /** Deny-list counterpart to {@link #getSupportedEngines()}. */
  default String[] getExcludedEngines() {
    return new String[0];
  }

  /** Setter counterpart to {@link #getExcludedEngines()}. Default no-op (see setter docs). */
  default void setExcludedEngines(String[] excludedEngines) {
    // intentional no-op default
  }

  /**
   * English-locale aliases for this plugin's name, category and keywords. These let the context
   * dialog match a transform/action by its original English term even when the UI runs in another
   * language (issue #2633). Empty when the UI is already English (the localized values are the
   * English ones) or for external implementations that don't participate.
   *
   * <p>Default returns an empty array so existing external {@link IPlugin} implementations stay
   * binary-compatible.
   */
  default String[] getEnglishKeywords() {
    return new String[0];
  }

  /**
   * Setter counterpart to {@link #getEnglishKeywords()}. Default no-op so external {@link IPlugin}
   * implementers don't have to participate — concrete plugins ({@link Plugin}) override this.
   */
  default void setEnglishKeywords(String[] englishKeywords) {
    // intentional no-op default
  }

  /**
   * A flag to indicate that the plugin needs libraries outside of the plugin folder
   *
   * @return true if there are extra libraries that need to be included outside the plugin folder
   */
  boolean isUsingLibrariesOutsidePluginFolder();

  /**
   * @return True if the JDBC drivers have to be loaded for this transform
   */
  boolean isIncludeJdbcDrivers();
}
