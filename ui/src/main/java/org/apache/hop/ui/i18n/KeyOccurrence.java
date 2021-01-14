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

import org.apache.commons.vfs2.FileObject;

/**
 * Contains the occurrence of a key in a java source code file
 *
 * @author matt
 * @since 2007-09-29
 */

public class KeyOccurrence implements Comparable<KeyOccurrence> {
  /**
   * The java source file
   */
  private FileObject fileObject;

  /**
   * The source folder the messages and java file live in
   */
  private String sourceFolder;

  /**
   * The location of the messages file, derived from "^import .*Messages;"
   */
  private String messagesPackage;

  /**
   * The index in the file on which the occurrence takes place
   */
  private int fileIndex;

  /**
   * The i18n key
   */
  private String key;

  /**
   * The arguments from the source code
   */
  private String arguments;

  /**
   * The number of occurrences
   */
  private int occurrences;

  /**
   * line of source code on which the key occurs.
   */
  private String sourceLine;

  public KeyOccurrence() {
    occurrences = 0;
  }

  /**
   * @param fileObject      The java source file
   * @param messagesPackage The location of the messages file, derived from "^import .*Messages;"
   * @param fileIndex       The position in the file
   * @param key             The i18n key
   * @param arguments       The arguments from the source code
   */
  public KeyOccurrence( FileObject fileObject, String sourceFolder, String messagesPackage, int fileIndex,
                        String key, String arguments, String sourceLine ) {
    this();
    if (fileObject == null) {
      throw new RuntimeException("A key occurrence needs to have a file in which it occurs");
    }
    if (messagesPackage == null) {
      throw new RuntimeException("A key occurrence needs to have a messages package in file '"+fileObject+"'");
    }

    this.fileObject = fileObject;
    this.sourceFolder = sourceFolder;
    this.messagesPackage = messagesPackage;
    this.fileIndex = fileIndex;
    this.key = key;
    this.arguments = arguments;
    this.occurrences = 1;
    this.sourceLine = sourceLine;
  }

  public String toString() {
    return "[source=" + sourceFolder + ", key=" + key + ", messages package=" + messagesPackage + "]";
  }

  public boolean equals( Object occ ) {
    if ( occ == null ) {
      return false;
    }
    if ( this == occ ) {
      return true;
    }
    return sourceFolder.equals( ( (KeyOccurrence) occ ).sourceFolder )
      && key.equals( ( (KeyOccurrence) occ ).key )
      && messagesPackage.equals( ( (KeyOccurrence) occ ).messagesPackage );
  }

  public int compareTo( KeyOccurrence occ ) {
    int cmp = key.compareTo( occ.key );
    if ( cmp != 0 ) {
      return cmp;
    }

    cmp = messagesPackage.compareTo( occ.messagesPackage );
    return cmp;
  }

  /**
   * @return The java source file
   */
  public FileObject getFileObject() {
    return fileObject;
  }

  /**
   * @param fileObject The java source file
   */
  public void setFileObject( FileObject fileObject ) {
    this.fileObject = fileObject;
  }

  /**
   * @return The location of the messages file
   */
  public String getMessagesPackage() {
    return messagesPackage;
  }

  /**
   * @param messagesPackage The location of the messages file
   */
  public void setMessagesPackage( String messagesPackage ) {
    this.messagesPackage = messagesPackage;
  }

  public String getSourceFolder() {
    return sourceFolder;
  }

  public void setSourceFolder( String sourceFolder ) {
    this.sourceFolder = sourceFolder;
  }

  /**
   * Gets fileIndex
   *
   * @return value of fileIndex
   */
  public int getFileIndex() {
    return fileIndex;
  }

  /**
   * @param fileIndex The fileIndex to set
   */
  public void setFileIndex( int fileIndex ) {
    this.fileIndex = fileIndex;
  }

  /**
   * @return The i18n key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key The i18n key
   */
  public void setKey( String key ) {
    this.key = key;
  }

  /**
   * @return The arguments from the source code
   */
  public String getArguments() {
    return arguments;
  }

  /**
   * @param arguments The arguments from the source code
   */
  public void setArguments( String arguments ) {
    this.arguments = arguments;
  }

  /**
   * @return The number of occurrences
   */
  public int getOccurrences() {
    return occurrences;
  }

  /**
   * @param occurrences The number of occurrences
   */
  public void setOccurrences( int occurrences ) {
    this.occurrences = occurrences;
  }

  /**
   * Increment the number of occurrences with one.
   */
  public void incrementOccurrences() {
    this.occurrences++;
  }

  /**
   * @return the line of source code on which the key occurs.
   */
  public String getSourceLine() {
    return sourceLine;
  }

  /**
   * @param sourceLine the line of source code on which the key occurs.
   */
  public void setSourceLine( String sourceLine ) {
    this.sourceLine = sourceLine;
  }
}
