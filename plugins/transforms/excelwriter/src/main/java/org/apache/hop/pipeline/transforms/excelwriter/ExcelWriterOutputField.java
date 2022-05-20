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

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ExcelWriterOutputField implements Cloneable {

  @HopMetadataProperty(injectionKey = "NAME",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.FieldName.Field")
  private String name;

  @HopMetadataProperty(injectionKey = "TYPE",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Type.Field")
  private String type;

  @HopMetadataProperty(injectionKey = "FORMAT",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Format.Field")
  private String format;

  @HopMetadataProperty(injectionKey = "FIELDTITLE",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Title.Field")
  private String title;

  @HopMetadataProperty(injectionKey = "FORMULA",
      injectionKeyDescription = "ExcelWriterMeta.Injection.Output.FieldContainFormula.Field")
  private boolean formula;

  @HopMetadataProperty(injectionKey = "HYPERLINKFIELD",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Hyperlink.Field")
  private String hyperlinkField;

  @HopMetadataProperty(injectionKey = "CELLCOMMENT",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.Comment.Field")
  private String commentField;

  @HopMetadataProperty(injectionKey = "COMMENTAUTHOR",
      injectionKeyDescription = "ExcelWriterMeta.Injection.Output.CommentAuthor.Field")
  private String commentAuthorField;

  @HopMetadataProperty(injectionKey = "TITLESTYLE",
      injectionKeyDescription = "ExcelWriterMeta.Injection.Output.TitleStyleCell.Field")
  private String titleStyleCell;

  @HopMetadataProperty(injectionKey = "STYLECELL",
          injectionKeyDescription = "ExcelWriterMeta.Injection.Output.StyleCell.Field")
  private String styleCell;

  public String getCommentAuthorField() {
    return commentAuthorField;
  }

  public void setCommentAuthorField(String commentAuthorField) {
    this.commentAuthorField = commentAuthorField;
  }

  public ExcelWriterOutputField(String name, String type, String format) {
    this.name = name;
    this.type = type;
    this.format = format;
  }

  public ExcelWriterOutputField(String name, String type, String format, String title, boolean formula, String hyperlinkField, String commentField, String commentAuthorField, String titleStyleCell, String styleCell) {
    this.name = name;
    this.type = type;
    this.format = format;
    this.title = title;
    this.formula = formula;
    this.hyperlinkField = hyperlinkField;
    this.commentField = commentField;
    this.commentAuthorField = commentAuthorField;
    this.titleStyleCell = titleStyleCell;
    this.styleCell = styleCell;
  }

  public ExcelWriterOutputField() {}

  public int compare(Object obj) {
    ExcelWriterOutputField field = (ExcelWriterOutputField) obj;

    return name.compareTo(field.getName());
  }

  @Override
  public boolean equals(Object obj) {
    ExcelWriterOutputField field = (ExcelWriterOutputField) obj;

    return field != null && name.equals(field.getName());
  }

  public boolean equalsAll(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExcelWriterOutputField that = (ExcelWriterOutputField) o;
    return formula == that.formula && name.equals(that.name) && type.equals(that.type) && format.equals(that.format) && title.equals(that.title) && hyperlinkField.equals(that.hyperlinkField) && commentField.equals(that.commentField) && commentAuthorField.equals(that.commentAuthorField) && titleStyleCell.equals(that.titleStyleCell) && styleCell.equals(that.styleCell);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String fieldname) {
    this.name = fieldname;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public boolean isFormula() {
    return formula;
  }

  public void setFormula(boolean formula) {
    this.formula = formula;
  }

  public String getHyperlinkField() {
    return hyperlinkField;
  }

  public void setHyperlinkField(String hyperlinkField) {
    this.hyperlinkField = hyperlinkField;
  }

  public String getCommentField() {
    return commentField;
  }

  public void setCommentField(String commentField) {
    this.commentField = commentField;
  }

  public String getTitleStyleCell() {
    return titleStyleCell;
  }

  public void setTitleStyleCell(String formatCell) {
    this.titleStyleCell = formatCell;
  }

  public String getStyleCell() {
    return styleCell;
  }

  public void setStyleCell(String styleCell) {
    this.styleCell = styleCell;
  }

  @Override
  public String toString() {
    return name + ":" + getType();
  }
}
