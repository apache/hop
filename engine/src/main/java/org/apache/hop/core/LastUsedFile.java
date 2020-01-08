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

package org.apache.hop.core;

import org.apache.hop.i18n.BaseMessages;

import java.util.Date;

public class LastUsedFile {

  private static Class<?> PKG = LastUsedFile.class;

  public static final String FILE_TYPE_TRANSFORMATION = "Trans";
  public static final String FILE_TYPE_JOB = "Job";
  public static final String FILE_TYPE_SCHEMA = "Schema";
  public static final String FILE_TYPE_CUSTOM = "Custom";

  public static final int OPENED_ITEM_TYPE_MASK_NONE = 0;
  public static final int OPENED_ITEM_TYPE_MASK_GRAPH = 1;
  public static final int OPENED_ITEM_TYPE_MASK_LOG = 2;
  public static final int OPENED_ITEM_TYPE_MASK_HISTORY = 4;

  private String fileType;
  private String filename;
  private Date lastOpened;

  private boolean opened;
  private int openItemTypes;

  /**
   * @param fileType      The type of file to use (FILE_TYPE_TRANSFORMATION, FILE_TYPE_JOB, ...)
   * @param filename
   * @param opened
   * @param openItemTypes
   * @param lastOpened
   */
  public LastUsedFile( String fileType, String filename, boolean opened, int openItemTypes, Date lastOpened ) {
    this.fileType = fileType;
    this.filename = filename;
    this.opened = opened;
    this.openItemTypes = openItemTypes;
    this.lastOpened = lastOpened == null ? new Date() : lastOpened;
  }

  public String toString() {
    return filename;
  }

  public int hashCode() {
    return ( getFileType() + toString() ).hashCode();
  }

  public boolean equals( Object obj ) {
    LastUsedFile file = (LastUsedFile) obj;
    return getFileType().equals( file.getFileType() ) && toString().equals( file.toString() );
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @return the fileType
   */
  public String getFileType() {
    return fileType;
  }

  public String getLongFileType() {
    String fileType = BaseMessages.getString( PKG, "System.FileType.File" );
    if ( FILE_TYPE_TRANSFORMATION.equals( getFileType() ) ) {
      fileType = BaseMessages.getString( PKG, "System.FileType.Transformation" );
    } else if ( FILE_TYPE_JOB.equals( getFileType() ) ) {
      fileType = BaseMessages.getString( PKG, "System.FileType.Job" );
    } else if ( FILE_TYPE_SCHEMA.equals( getFileType() ) ) {
      fileType = FILE_TYPE_SCHEMA;
    }
    return fileType;
  }

  /**
   * @param fileType the fileType to set
   */
  public void setFileType( String fileType ) {
    this.fileType = fileType;
  }

  public boolean isTransformation() {
    return FILE_TYPE_TRANSFORMATION.equalsIgnoreCase( fileType );
  }

  public boolean isJob() {
    return FILE_TYPE_JOB.equalsIgnoreCase( fileType );
  }

  public boolean isSchema() {
    return FILE_TYPE_SCHEMA.equalsIgnoreCase( fileType );
  }

  /**
   * @return the opened
   */
  public boolean isOpened() {
    return opened;
  }

  /**
   * @param opened the opened to set
   */
  public void setOpened( boolean opened ) {
    this.opened = opened;
  }

  /**
   * @return the openItemTypes
   */
  public int getOpenItemTypes() {
    return openItemTypes;
  }

  public Date getLastOpened() {
    return lastOpened;
  }

  public void setLastOpened( Date lastOpened ) {
    this.lastOpened = lastOpened;
  }

}
