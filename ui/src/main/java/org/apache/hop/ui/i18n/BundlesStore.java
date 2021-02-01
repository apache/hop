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

import org.apache.hop.core.exception.HopException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BundlesStore {

  private List<String> bundleRootFolders;

  /*
    This map contains the content of message bundles per package, per language and per file
   */
  private Map<String, Map<String, BundleFile>> packageLanguageBundleMap;

  public BundlesStore() {
    this.bundleRootFolders = new ArrayList<>();
    this.packageLanguageBundleMap = new HashMap<>();
  }

  public BundlesStore( List<String> bundleRootFolders ) {
    this();
    this.bundleRootFolders = bundleRootFolders;
  }

  /**
   * Let's look at all the messages bundle package root folders.
   * We only look at src/main/resources/
   * Avoid impl folders
   *
   * @param rootFolder
   */
  public BundlesStore( String rootFolder ) throws HopException {
    this();
    try {
      Files
        .walk( Paths.get( rootFolder ) )
        .filter( path ->
          Files.isDirectory( path ) &&
            path.endsWith( "src/main/resources" ) &&
            !path.toString().contains( "/impl/" )
        )
        .forEach( path -> bundleRootFolders.add( path.toAbsolutePath().toFile().getPath() ) );
    } catch ( IOException e ) {
      throw new HopException( "Error reading root folder: " + rootFolder, e );
    }    
  }

  public void findAllMessagesBundles() throws HopException {
    try {
      for ( String bundleRootFolder : bundleRootFolders ) {
        Files.walk( Paths.get( bundleRootFolder ) )
          .filter( path ->
            Files.isRegularFile( path ) &&
              path.getFileName().toString().startsWith( "messages_" ) &&
              path.getFileName().toString().endsWith( ".properties" )
          )
          .forEach( path ->
            addMessagesFile( bundleRootFolder, path )
          )
        ;
      }
    } catch ( Exception e ) {
      throw new HopException( "Error searching for messages bundles", e );
    }
  }

  /**
   * @param bundleRootFolder
   * @param messagesFilePath
   */
  private void addMessagesFile( String bundleRootFolder, Path messagesFilePath ) throws RuntimeException {
    // Root folder :    /home/matt/git/project-hop/hop/ui/src/main/resources
    // Messages folder: /home/matt/git/project-hop/hop/ui/src/main/resources/org/apache/hop/ui/hopgui/messages/
    //

    String messagesFileFolder = messagesFilePath.toFile().getParent();
    if ( messagesFileFolder.startsWith( bundleRootFolder ) ) {
      // We can determine the package...
      //
      String packageName = messagesFileFolder
        .substring( bundleRootFolder.length() )
        .replace(File.separator,"/")
        .replaceAll( "\\/messages$", "" )
        .replaceAll( "^\\/", "" )
        .replaceAll( "\\/", "." );

      // What is the language?
      // Decompose messages_en_US.properties to en_US using some regex
      //
      String locale = messagesFilePath
        .getFileName().toString()
        .replaceAll( "^messages_", "" )
        .replaceAll( "\\.properties$", "" );

      // Now store this bundle in the store...
      //
      String filename = messagesFilePath.toString();
      try {
        BundleFile bundleFile = new BundleFile( filename, packageName, locale );
        addBundleFile( packageName, locale, bundleFile );
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to read messages bundle file : " + filename, e );
      }
    }
  }

  public void addBundleFile( String packageName, String language, BundleFile bundleFile ) {
    // Find the languages for the package:
    //
    Map<String, BundleFile> languageBundleMap = packageLanguageBundleMap.get( packageName );
    if ( languageBundleMap == null ) {
      languageBundleMap = new HashMap<>();
      packageLanguageBundleMap.put( packageName, languageBundleMap );
    }

    // Find the bundle file for the language:
    // If it already exists we're storing messages bundles twice or something like that.
    //
    BundleFile existingFile = languageBundleMap.get( language );
    if ( existingFile != null ) {
      throw new RuntimeException( "Bundle file collission!  "
        + "We're trying to add file '" + bundleFile.getFilename() + "' to the bundle store but this file already exists "
        + "in the same package '" + packageName + "' for the same language '" + language + "' : '" + existingFile.getFilename() + "'" );
    }

    languageBundleMap.put( language, bundleFile );
  }


  public Map<String, String> findTranslations( String packageName, String key ) {
    Map<String, String> translations = new HashMap<>();
    Map<String, BundleFile> languageBundleFileMap = packageLanguageBundleMap.get( packageName );
    if ( languageBundleFileMap != null ) {
      for ( String language : languageBundleFileMap.keySet() ) {
        String translation = languageBundleFileMap.get( language ).get( key );
        if ( translation != null ) {
          translations.put( language, translation );
        }
      }
    }
    return translations;
  }


  public String lookupTranslation( String packageName, String locale, String key ) {
    Map<String, BundleFile> languageBundleFileMap = packageLanguageBundleMap.get( packageName );
    if ( languageBundleFileMap != null ) {
      BundleFile bundleFile = languageBundleFileMap.get( locale );
      if ( bundleFile != null ) {
        return bundleFile.get( key );
      }
    }
    return null;
  }


  public void removeTranslation( String packageName, String locale, String key ) {
    Map<String, BundleFile> languageBundleFileMap = packageLanguageBundleMap.get( packageName );
    if ( languageBundleFileMap != null ) {
      BundleFile bundleFile = languageBundleFileMap.get( locale );
      if ( bundleFile != null ) {
        bundleFile.remove( key );
      }
    }
  }

  public void addPipelinelation( String sourceFolder, String packageName, String locale, String key, String value ) {
    Map<String, BundleFile> languageBundleFileMap = packageLanguageBundleMap.get( packageName );
    if ( languageBundleFileMap == null ) {
      languageBundleFileMap = new HashMap<>();
      packageLanguageBundleMap.put( packageName, languageBundleFileMap );
    }

    BundleFile bundleFile = languageBundleFileMap.get( locale );
    if ( bundleFile == null ) {
      // Create a new bundle file
      //
      // Calculate the resources folder based off the Java folder
      // sourceFolder would be /path/plugins/databases/firebird/src/main/java
      // We need to come up with /path/plugins/databases/firebird/src/main/resources
      // So : replace "java" with "resources"
      //
      String bundleFileName = sourceFolder.replaceAll( "\\/java", "/resources/" );
      bundleFileName += packageName.replaceAll( "\\.", "/" );
      bundleFileName += "/messages/messages_" + locale + ".properties";

      // TODO finish/test calculating filename
      bundleFile = new BundleFile( bundleFileName, packageName, locale, new HashMap<>() );
      languageBundleFileMap.put( locale, bundleFile );
    }
    bundleFile.put( key, value );
  }


  public List<BundleFile> getChangedBundleFiles() {
    List<BundleFile> bundleFiles = new ArrayList<>();
    for ( String packageName : packageLanguageBundleMap.keySet() ) {
      Map<String, BundleFile> languageBundleMap = packageLanguageBundleMap.get( packageName );
      for ( String locale : languageBundleMap.keySet() ) {
        BundleFile bundleFile = languageBundleMap.get( locale );
        if ( bundleFile.hasChanged() ) {
          bundleFiles.add( bundleFile );
        }
      }
    }
    return bundleFiles;
  }

  /**
   * Get the bundle file for the given package of all bundles for the locale if the package name is null.
   * @param packageName The package name or null if you want all files for a locale
   * @param locale The locale to search for
   * @return The bundle files
   */
  public List<BundleFile> getBundleFiles( String packageName, String locale ) {
    List<BundleFile> bundleFiles = new ArrayList<>();

    for ( String pckName : packageLanguageBundleMap.keySet() ) {
      if ( packageName == null || packageName.equals( pckName ) ) {
        Map<String, BundleFile> languageBundleMap = packageLanguageBundleMap.get( pckName );
        if ( languageBundleMap != null ) {
          BundleFile bundleFile = languageBundleMap.get( locale );
          if ( bundleFile != null ) {
            bundleFiles.add( bundleFile );
          }
        }
      }
    }

    return bundleFiles;
  }

  public BundleFile findBundleFile( String packageName, String locale ) {
    Map<String, BundleFile> languageBundleMap = packageLanguageBundleMap.get( packageName );
    if (languageBundleMap!=null) {
      return languageBundleMap.get(locale);
    }
    return null;
  }


  /**
   * Gets bundleRootFolders
   *
   * @return value of bundleRootFolders
   */
  public List<String> getBundleRootFolders() {
    return bundleRootFolders;
  }

  /**
   * @param bundleRootFolders The bundleRootFolders to set
   */
  public void setBundleRootFolders( List<String> bundleRootFolders ) {
    this.bundleRootFolders = bundleRootFolders;
  }

  /**
   * Gets packageLanguageBundleMap
   *
   * @return value of packageLanguageBundleMap
   */
  public Map<String, Map<String, BundleFile>> getPackageLanguageBundleMap() {
    return packageLanguageBundleMap;
  }

  /**
   * @param packageLanguageBundleMap The packageLanguageBundleMap to set
   */
  public void setPackageLanguageBundleMap( Map<String, Map<String, BundleFile>> packageLanguageBundleMap ) {
    this.packageLanguageBundleMap = packageLanguageBundleMap;
  }
}
