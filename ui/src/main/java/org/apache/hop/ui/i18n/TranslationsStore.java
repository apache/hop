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

package org.apache.hop.ui.i18n;

import org.apache.hop.core.logging.ILogChannel;

import java.util.List;
import java.util.Map;

/**
 * This class contains and handles all the translations for the keys specified in the Java source code.
 *
 * @author matt
 */
public class TranslationsStore {

  private List<String> localeList;

  private String mainLocale;

  private Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences;
  private final BundlesStore bundleStore;

  private ILogChannel log;

  /**
   *  @param log
   * @param localeList
   * @param mainLocale
   * @param sourcePackageOccurrences
   * @param bundlesStore
   */
  public TranslationsStore( ILogChannel log, List<String> localeList, String mainLocale,
                            Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences, BundlesStore bundlesStore ) {
    super();
    this.log = log;
    this.localeList = localeList;
    this.mainLocale = mainLocale;
    this.sourcePackageOccurrences = sourcePackageOccurrences;
    this.bundleStore = bundlesStore;
  }

  /**
   * Look up the translation for a key in a certain locale
   *
   * @param locale          the locale to hunt for
   * @param messagesPackage the messages package to look in
   * @param key             the key
   * @return the translation for the specified key in the desired locale, from the requested package
   */
  public String lookupKeyValue( String locale, String messagesPackage, String key ) {
    return bundleStore.lookupTranslation(messagesPackage, locale, key);
  }

  public void removeValue( String locale, String sourceFolder, String messagesPackage, String key ) {
    bundleStore.removeTranslation(messagesPackage, locale, key);
  }

  public void storeValue( String locale, String sourceFolder, String messagesPackage, String key, String value ) {
    bundleStore.addPipelinelation(sourceFolder, messagesPackage, locale, key, value);
  }

  /**
   * @return the list of changed messages stores.
   */
  public List<BundleFile> getChangedBundleFiles() {
    return bundleStore.getChangedBundleFiles();
  }

  /**
   * @param searchLocale    the locale the filter on.
   * @param messagesPackage the messagesPackage to filter on. Specify null to get all message stores.
   * @return the list of messages bundle files for the locale
   */
  public List<BundleFile> findBundleFiles( String searchLocale, String messagesPackage ) {
    return bundleStore.getBundleFiles(searchLocale, messagesPackage);
  }

  public BundleFile findMainBundleFile( String messagesPackage ) {
    List<BundleFile> bundlesFiles = findBundleFiles( mainLocale, messagesPackage );
    if (bundlesFiles.isEmpty()) {
      return null;
    }
    return bundlesFiles.get(0);
  }

  /**
   * @return the localeList
   */
  public List<String> getLocaleList() {
    return localeList;
  }

  /**
   * @param localeList the localeList to set
   */
  public void setLocaleList( List<String> localeList ) {
    this.localeList = localeList;
  }

  /**
   * @return the mainLocale
   */
  public String getMainLocale() {
    return mainLocale;
  }

  /**
   * @param mainLocale the mainLocale to set
   */
  public void setMainLocale( String mainLocale ) {
    this.mainLocale = mainLocale;
  }


  public Map<String, Map<String, List<KeyOccurrence>>> getSourcePackageOccurrences() {
    return sourcePackageOccurrences;
  }

  public void setSourcePackageOccurrences( Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences ) {
    this.sourcePackageOccurrences = sourcePackageOccurrences;
  }

  /**
   * Gets bundleStore
   *
   * @return value of bundleStore
   */
  public BundlesStore getBundleStore() {
    return bundleStore;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }
}
