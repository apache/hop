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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class ExcelWriterOutputField implements Cloneable {

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.FieldName.Field")
  private String name;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Type.Field")
  private String type;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Format.Field")
  private String format;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Title.Field")
  private String title;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.FieldContainFormula.Field")
  private boolean formula;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Hyperlink.Field")
  private String hyperlinkField;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Comment.Field")
  private String commentField;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.CommentAuthor.Field")
  private String commentAuthorField;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.TitleStyleCell.Field")
  private String titleStyleCell;

  @HopMetadataProperty(
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.StyleCell.Field")
  private String styleCell;

  public String getCommentAuthorField() {
    return commentAuthorField;
  }

  public void setCommentAuthorField( String commentAuthorField ) {
    this.commentAuthorField = commentAuthorField;
  }

  public ExcelWriterOutputField(String name, String type, String format ) {
    this.name = name;
    this.type = type;
    this.format = format;
  }

  public ExcelWriterOutputField() {
  }

  public int compare( Object obj ) {
    ExcelWriterOutputField field = (ExcelWriterOutputField) obj;

    return name.compareTo( field.getName() );
  }

  @Override
  public boolean equals( Object obj ) {
    ExcelWriterOutputField field = (ExcelWriterOutputField) obj;

    return name.equals( field.getName() );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Deprecated
  public boolean equal( Object obj ) {
    return equals( obj );
  }

  @Override
  public Object clone() {
    try {
      Object retval = super.clone();
      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public String getName() {
    return name;
  }

  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle( String title ) {
    this.title = title;
  }

  public boolean isFormula() {
    return formula;
  }

  public void setFormula( boolean formula ) {
    this.formula = formula;
  }

  public String getHyperlinkField() {
    return hyperlinkField;
  }

  public void setHyperlinkField( String hyperlinkField ) {
    this.hyperlinkField = hyperlinkField;
  }

  public String getCommentField() {
    return commentField;
  }

  public void setCommentField( String commentField ) {
    this.commentField = commentField;
  }

  public String getTitleStyleCell() {
    return titleStyleCell;
  }

  public void setTitleStyleCell( String formatCell ) {
    this.titleStyleCell = formatCell;
  }

  public String getStyleCell() {
    return styleCell;
  }

  public void setStyleCell( String styleCell ) {
    this.styleCell = styleCell;
  }

  @Override
  public String toString() {
    return name + ":" + getType();
  }
}
