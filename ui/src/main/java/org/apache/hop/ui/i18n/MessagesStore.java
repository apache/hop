/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.i18n;

import org.apache.hop.core.Const;
import org.apache.hop.core.changed.ChangedFlag;
import org.apache.hop.core.exception.HopException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class contains a messages store: for a certain Locale and for a certain messages package, it keeps all the keys
 * and values. This class can read and write messages files...
 *
 * @author matt
 */
public class MessagesStore extends ChangedFlag {

  private String sourceFolder;

  private String locale;

  private String messagesPackage;

  private Map<String, String> messagesMap;

  private String filename;

  private Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences;

  public MessagesStore() {
    this.messagesMap = new HashMap<>();
    this.sourcePackageOccurrences = new HashMap<>();
  }

  /**
   * Create a new messages store
   *
   * @param locale                   The locale to read
   * @param sourceFolder             The source folder to read
   * @param messagesPackage          The messages package to consider
   * @param sourcePackageOccurrences The occurrences map
   */
  public MessagesStore( String locale, String sourceFolder, String messagesPackage,
                        Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences ) {
    this();
    this.sourceFolder = sourceFolder;
    this.locale = locale;
    this.messagesPackage = messagesPackage;
    this.sourcePackageOccurrences = sourcePackageOccurrences;

    if ( sourceFolder == null ) {
      throw new RuntimeException( "Source folder can not be null, messages package : "
        + messagesPackage + ", locale: " + locale );
    }
  }

  /**
   * Determine locale and package from filename
   *
   * @param bundleRootFolder
   * @param filename
   */
  public MessagesStore( String bundleRootFolder, String filename ) {
    this();
    this.filename = filename;

    String folderName = new File(filename).getParentFile().getPath();
    if (folderName.startsWith( bundleRootFolder )) {
      messagesPackage = folderName.substring( bundleRootFolder.length() ).replace( '/', '.' ).replaceAll("^\\.", "").replaceAll("\\.messages$", "");
      System.out.println("Messages file '"+filename+"' found, package is : '"+messagesPackage+"'");
    }
  }

  public void read( List<String> directories ) throws HopException {
    try {
      filename = getLoadFilename( directories );

      Properties properties = new Properties();
      FileInputStream fileInputStream = new FileInputStream( new File( filename ) );
      properties.load( fileInputStream );
      fileInputStream.close();

      // Put all the properties in our map...
      //
      for ( Object key : properties.keySet() ) {
        Object value = properties.get( key );
        messagesMap.put( (String) key, (String) value );
      }
    } catch ( Exception e ) {
      String keys = "[";
      Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get( sourceFolder );
      List<KeyOccurrence> keyList = po == null ? new ArrayList<KeyOccurrence>() : po.get( messagesPackage );
      if ( keyList == null ) {
        keyList = new ArrayList<KeyOccurrence>();
      }
      boolean first = true;
      for ( KeyOccurrence occ : keyList ) {
        if ( first ) {
          first = false;
        } else {
          keys += ", ";
        }
        keys += occ.getKey() + "/" + occ.getFileObject().toString();
      }
      keys += "]";
      throw new HopException( "Unable to read messages file for locale : '"
        + locale + "' and package '" + messagesPackage + "', keys=" + keys, e );
    }
  }

  public void write() throws HopException {
    if ( filename == null ) {
      throw new HopException( "Please specify a filename before saving messages store for package '"
        + messagesPackage + "' and locale '" + locale + "" );
    }
    write( filename );
  }

  public void write( String filename ) throws HopException {
    try {
      File file = new File( filename );
      if ( !file.exists() ) {
        File parent = file.getParentFile();
        if ( !parent.exists() ) {
          parent.mkdirs(); // create the messages/ folder
        }
      }
      Properties properties = new Properties();
      for ( String key : messagesMap.keySet() ) {
        properties.put( key, messagesMap.get( key ) );
      }
      FileOutputStream fileOutputStream = new FileOutputStream( file );
      String comment =
        "File generated by Hop Translator for package '"
          + messagesPackage + "' in locale '" + locale + "'" + Const.CR + Const.CR;
      properties.store( fileOutputStream, comment );
      fileOutputStream.close();
      setChanged( false );
    } catch ( IOException e ) {
      throw new HopException( "Unable to save messages properties file '" + filename + "'", e );
    }
  }

  /**
   * Find a suitable filename for the specified locale and messages package. It tries to find the file in the specified
   * directories in the order that they are specified.
   *
   * @param alternativeSourceFolders
   * @param directories              the source directories to try and map the messages files against.
   * @return the filename that was found.
   */
  public String getLoadFilename( List<String> alternativeSourceFolders ) throws FileNotFoundException {
    String path = calcFolderName( sourceFolder );

    // First try the source folder of the Java file
    //
    if ( new File( path ).exists() ) {
      return path;
    }

    // Then try the rest of the project source folders in order of occurrence ..
    //
    for ( String altSourceFolder : alternativeSourceFolders ) {
      path = calcFolderName( altSourceFolder );
      if ( new File( path ).exists() ) {
        return path;
      }
    }
    throw new FileNotFoundException( "package file could not be found for file in folder "
      + sourceFolder + ", with messages package " + messagesPackage + " in locale " + locale );
  }

  private String calcFolderName( String sourceFolderPath ) {
    String localeUpperLower = locale.substring( 0, 3 ).toLowerCase() + locale.substring( 3 ).toUpperCase();
    String filename = "messages_" + localeUpperLower + ".properties";
    String path =
      sourceFolderPath
        + File.separator + messagesPackage.replace( '.', '/' ) + File.separator + "messages" + File.separator
        + filename;
    return path;
  }

  public String getSourceDirectory( List<String> directories ) {
    String localeUpperLower = locale.substring( 0, 3 ).toLowerCase() + locale.substring( 3 ).toUpperCase();

    String filename = "messages_" + localeUpperLower + ".properties";
    String path = messagesPackage.replace( '.', '/' );

    for ( String directory : directories ) {
      String attempt =
        directory
          + Const.FILE_SEPARATOR + path + Const.FILE_SEPARATOR + "messages" + Const.FILE_SEPARATOR + filename;
      if ( new File( attempt ).exists() ) {
        return directory;
      }
    }
    return null;
  }

  /**
   * Find a suitable filename to save this information in the specified locale and messages package. It needs a source
   * directory to save the package in
   *
   * @param directory the source directory to save the messages file in.
   * @return the filename that was generated.
   */
  public String getSaveFilename( String directory ) {
    String localeUpperLower = locale.substring( 0, 3 ).toLowerCase() + locale.substring( 3 ).toUpperCase();

    String filename = "messages_" + localeUpperLower + ".properties";
    String path = messagesPackage.replace( '.', '/' );

    return directory
      + Const.FILE_SEPARATOR + path + Const.FILE_SEPARATOR + "messages" + Const.FILE_SEPARATOR + filename;
  }

  /**
   * @return the locale
   */
  public String getLocale() {
    return locale;
  }

  /**
   * @param locale the locale to set
   */
  public void setLocale( String locale ) {
    this.locale = locale;
  }

  /**
   * @return the messagesPackage
   */
  public String getMessagesPackage() {
    return messagesPackage;
  }

  /**
   * @param messagesPackage the messagesPackage to set
   */
  public void setMessagesPackage( String messagesPackage ) {
    this.messagesPackage = messagesPackage;
  }

  /**
   * @return the map
   */
  public Map<String, String> getMessagesMap() {
    return messagesMap;
  }

  /**
   * @param messsagesMap the map to set
   */
  public void setMessagesMap( Map<String, String> messsagesMap ) {
    this.messagesMap = messsagesMap;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getSourceFolder() {
    return sourceFolder;
  }

  public void setSourceFolder( String sourceFolder ) {
    this.sourceFolder = sourceFolder;
  }

}
