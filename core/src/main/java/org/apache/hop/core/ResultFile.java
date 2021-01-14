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

package org.apache.hop.core;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

import java.util.Date;

/**
 * This is a result file: a file as a result of the execution of a action, a pipeline transform, etc.
 *
 * @author matt
 */
public class ResultFile implements Cloneable {
  private static final Class<?> PKG = Const.class; // For Translator

  public static final int FILE_TYPE_GENERAL = 0;
  public static final int FILE_TYPE_LOG = 1;
  public static final int FILE_TYPE_ERRORLINE = 2;
  public static final int FILE_TYPE_ERROR = 3;
  public static final int FILE_TYPE_WARNING = 4;

  public static final String[] fileTypeCode = { "GENERAL", "LOG", "ERRORLINE", "ERROR", "WARNING" };

  public static final String[] fileTypeDesc = {
    BaseMessages.getString( PKG, "ResultFile.FileType.General" ),
    BaseMessages.getString( PKG, "ResultFile.FileType.Log" ),
    BaseMessages.getString( PKG, "ResultFile.FileType.ErrorLine" ),
    BaseMessages.getString( PKG, "ResultFile.FileType.Error" ),
    BaseMessages.getString( PKG, "ResultFile.FileType.Warning" ) };
  private static final String XML_TAG = "result-file";

  private int type;
  private FileObject file;
  private String originParent;
  private String origin;
  private String comment;
  private Date timestamp;

  /**
   * Construct a new result file
   *
   * @param type         The type of file : FILE_TYPE_GENERAL, ...
   * @param file         The file to use
   * @param originParent The pipeline or workflow that has generated this result file
   * @param origin       The transform or action that has generated this result file
   */
  public ResultFile( int type, FileObject file, String originParent, String origin ) {
    this.type = type;
    this.file = file;
    this.originParent = originParent;
    this.origin = origin;
    this.timestamp = new Date();
  }

  @Override
  public String toString() {
    return file.toString()
      + " - " + getTypeDesc() + " - " + XmlHandler.date2string( timestamp )
      + ( origin == null ? "" : " - " + origin ) + ( originParent == null ? "" : " - " + originParent );

  }

  @Override
  protected ResultFile clone() throws CloneNotSupportedException {
    return (ResultFile) super.clone();
  }

  /**
   * @return Returns the comment.
   */
  public String getComment() {
    return comment;
  }

  /**
   * @param comment The comment to set.
   */
  public void setComment( String comment ) {
    this.comment = comment;
  }

  /**
   * @return Returns the file.
   */
  public FileObject getFile() {
    return file;
  }

  /**
   * @param file The file to set.
   */
  public void setFile( FileObject file ) {
    this.file = file;
  }

  /**
   * @return Returns the origin : the transform or action that generated this result file
   */
  public String getOrigin() {
    return origin;
  }

  /**
   * @param origin The origin to set : the transform or action that generated this result file
   */
  public void setOrigin( String origin ) {
    this.origin = origin;
  }

  /**
   * @return Returns the originParent : the pipeline or workflow that generated this result file
   */
  public String getOriginParent() {
    return originParent;
  }

  /**
   * @param originParent The originParent to set : the pipeline or workflow that generated this result file
   */
  public void setOriginParent( String originParent ) {
    this.originParent = originParent;
  }

  /**
   * @return Returns the type.
   */
  public int getType() {
    return type;
  }

  /**
   * @param type The type to set.
   */
  public void setType( int type ) {
    this.type = type;
  }

  /**
   * @return The description of this result files type.
   */
  public String getTypeDesc() {
    return fileTypeDesc[ type ];
  }

  public String getTypeCode() {
    return fileTypeCode[ type ];
  }

  /**
   * Search for the result file type, looking in both the descriptions (i18n depending) and the codes
   *
   * @param typeString the type string to search for
   * @return the result file type
   */
  public static final int getType( String typeString ) {
    int idx = Const.indexOfString( typeString, fileTypeDesc );
    if ( idx >= 0 ) {
      return idx;
    }
    idx = Const.indexOfString( typeString, fileTypeCode );
    if ( idx >= 0 ) {
      return idx;
    }

    return FILE_TYPE_GENERAL;
  }

  /**
   * @param fileType the result file type
   * @return the result file type code
   */
  public static final String getTypeCode( int fileType ) {
    return fileTypeCode[ fileType ];
  }

  /**
   * @param fileType the result file type
   * @return the result file type description
   */
  public static final String getTypeDesc( int fileType ) {
    return fileTypeDesc[ fileType ];
  }

  public static final String[] getAllTypeDesc() {
    return fileTypeDesc;
  }

  /**
   * @return Returns the timestamp.
   */
  public Date getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp The timestamp to set.
   */
  public void setTimestamp( Date timestamp ) {
    this.timestamp = timestamp;
  }

  /**
   * @return an output Row for this Result File object.
   */
  public RowMetaAndData getRow() {
    RowMetaAndData row = new RowMetaAndData();

    // First the type
    row.addValue( new ValueMetaString( "type" ), getTypeDesc() );

    // The filename
    row.addValue( new ValueMetaString( "filename" ), file.getName().getBaseName() );

    // The path
    row.addValue( new ValueMetaString( "path" ), file.getName().getURI() );

    // The origin parent
    row.addValue( new ValueMetaString( "parentorigin" ), originParent );

    // The origin
    row.addValue( new ValueMetaString( "origin" ), origin );

    // The comment
    row.addValue( new ValueMetaString( "comment" ), comment );

    // The timestamp
    row.addValue( new ValueMetaDate( "timestamp" ), timestamp );

    return row;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.openTag( XML_TAG ) );

    xml.append( XmlHandler.addTagValue( "type", getTypeCode() ) );
    xml.append( XmlHandler.addTagValue( "file", file.getName().toString() ) );
    xml.append( XmlHandler.addTagValue( "parentorigin", originParent ) );
    xml.append( XmlHandler.addTagValue( "origin", origin ) );
    xml.append( XmlHandler.addTagValue( "comment", comment ) );
    xml.append( XmlHandler.addTagValue( "timestamp", timestamp ) );

    xml.append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  public ResultFile( Node node ) throws HopFileException {
    try {
      type = getType( XmlHandler.getTagValue( node, "type" ) );
      file = HopVfs.getFileObject( XmlHandler.getTagValue( node, "file" ) );
      originParent = XmlHandler.getTagValue( node, "parentorigin" );
      origin = XmlHandler.getTagValue( node, "origin" );
      comment = XmlHandler.getTagValue( node, "comment" );
      timestamp = XmlHandler.stringToDate( XmlHandler.getTagValue( node, "timestamp" ) );
    } catch ( Exception e ) {
      throw new HopFileException( e );
    }
  }

}
