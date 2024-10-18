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

package org.apache.hop.pipeline.transforms.filemetadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "FileMetadataPlugin",
    name = "i18n::FileMetadata.Name",
    image = "filemetadata.svg",
    description = "i18n::FileMetadata.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::FileMetadata.Keyword",
    documentationUrl = "/pipeline/transforms/filemetadata.html")
public class FileMetadataMeta extends BaseTransformMeta<FileMetadata, FileMetadataData> {
  /** Stores the name of the file to examine */
  @HopMetadataProperty(key = "fileName")
  private String fileName;

  @HopMetadataProperty(key = "filenameInField")
  private boolean filenameInField;

  @HopMetadataProperty(key = "fileNameField")
  private String filenameField;

  @HopMetadataProperty(key = "limitRows")
  private String limitRows;

  @HopMetadataProperty(key = "defaultCharset")
  private String defaultCharset;

  // candidates for delimiters in delimited files
  @HopMetadataProperty(key = "delimiterCandidate")
  private List<FMCandidate> delimiterCandidates;

  // candidates for enclosure characters in delimited files
  @HopMetadataProperty(key = "enclosureCandidate")
  private List<FMCandidate> enclosureCandidates;

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public FileMetadataMeta() {
    super();
    this.delimiterCandidates = new ArrayList<>();
    this.enclosureCandidates = new ArrayList<>();
  }

  public FileMetadataMeta(FileMetadataMeta m) {
    this();
    this.fileName = m.fileName;
    this.limitRows = m.limitRows;
    this.defaultCharset = m.defaultCharset;
    m.delimiterCandidates.forEach(c -> this.delimiterCandidates.add(new FMCandidate(c)));
    m.enclosureCandidates.forEach(c -> this.enclosureCandidates.add(new FMCandidate(c)));
  }

  /**
   * This method is called every time a new transform is created and should allocate/set the
   * transform configuration to sensible defaults. The values set here will be used by Spoon when a
   * new transform is created.
   */
  @Override
  public void setDefault() {
    fileName = "";
    limitRows = "10000";
    defaultCharset = "ISO-8859-1";
    filenameInField = false;

    delimiterCandidates.clear();
    delimiterCandidates.add(new FMCandidate("\t"));
    delimiterCandidates.add(new FMCandidate(";"));
    delimiterCandidates.add(new FMCandidate(","));

    enclosureCandidates.clear();
    enclosureCandidates.add(new FMCandidate("\""));
    enclosureCandidates.add(new FMCandidate("'"));
  }

  /**
   * This method is used when a transform is duplicated. It needs to return a deep copy of this
   * object. Be sure to create proper deep copies if the transform configuration is stored in
   * modifiable objects.
   *
   * @return a deep copy of this
   */
  @Override
  public FileMetadataMeta clone() {
    return new FileMetadataMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    rowMeta.addRowMeta(
        new RowMetaBuilder()
            .addString("charset")
            .addString("delimiter")
            .addString("enclosure")
            .addInteger("field_count")
            .addInteger("skip_header_lines")
            .addInteger("skip_footer_lines")
            .addBoolean("header_line_present")
            .addString("name")
            .addString("type")
            .addInteger("length")
            .addInteger("precision")
            .addString("mask")
            .addString("decimal_symbol")
            .addString("grouping_symbol")
            .build());
  }

  public static final class FMCandidate {
    @HopMetadataProperty(key = "candidate")
    private String candidate;

    public FMCandidate() {}

    public FMCandidate(FMCandidate c) {
      this.candidate = c.candidate;
    }

    public FMCandidate(String candidate) {
      this.candidate = candidate;
    }

    /**
     * Gets candidate
     *
     * @return value of candidate
     */
    public String getCandidate() {
      return candidate;
    }

    /**
     * Sets candidate
     *
     * @param candidate value of candidate
     */
    public void setCandidate(String candidate) {
      this.candidate = candidate;
    }
  }

  /**
   * Gets fileName
   *
   * @return value of fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Sets fileName
   *
   * @param fileName value of fileName
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public boolean isFilenameInField() {
    return filenameInField;
  }

  public void setFilenameInField(boolean filenameInField) {
    this.filenameInField = filenameInField;
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets limitRows
   *
   * @return value of limitRows
   */
  public String getLimitRows() {
    return limitRows;
  }

  /**
   * Sets limitRows
   *
   * @param limitRows value of limitRows
   */
  public void setLimitRows(String limitRows) {
    this.limitRows = limitRows;
  }

  /**
   * Gets defaultCharset
   *
   * @return value of defaultCharset
   */
  public String getDefaultCharset() {
    return defaultCharset;
  }

  /**
   * Sets defaultCharset
   *
   * @param defaultCharset value of defaultCharset
   */
  public void setDefaultCharset(String defaultCharset) {
    this.defaultCharset = defaultCharset;
  }

  /**
   * Gets delimiterCandidates
   *
   * @return value of delimiterCandidates
   */
  public List<FMCandidate> getDelimiterCandidates() {
    return delimiterCandidates;
  }

  /**
   * Sets delimiterCandidates
   *
   * @param delimiterCandidates value of delimiterCandidates
   */
  public void setDelimiterCandidates(List<FMCandidate> delimiterCandidates) {
    this.delimiterCandidates = delimiterCandidates;
  }

  /**
   * Gets enclosureCandidates
   *
   * @return value of enclosureCandidates
   */
  public List<FMCandidate> getEnclosureCandidates() {
    return enclosureCandidates;
  }

  /**
   * Sets enclosureCandidates
   *
   * @param enclosureCandidates value of enclosureCandidates
   */
  public void setEnclosureCandidates(List<FMCandidate> enclosureCandidates) {
    this.enclosureCandidates = enclosureCandidates;
  }
}
