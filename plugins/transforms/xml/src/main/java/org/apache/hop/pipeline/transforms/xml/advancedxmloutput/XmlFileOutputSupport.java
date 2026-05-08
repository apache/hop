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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Self-contained holder of all filename / extension / split / date-time / zip / result-file options
 * used by {@link AdvancedXmlOutputMeta}. Kept as a nested-style POJO so it serializes under a
 * single {@code <file>} element, mirroring the convention used by the existing XML Output
 * transform.
 */
@Getter
@Setter
public class XmlFileOutputSupport {

  /** Base name of the output file. */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FILENAME",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.FILENAME")
  private String fileName;

  /** Optional file extension (without leading dot). */
  @HopMetadataProperty(
      key = "extension",
      injectionKey = "EXTENSION",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.EXTENSION")
  private String extension;

  /** Maximum number of input rows per file. 0 = unlimited (single file). */
  @HopMetadataProperty(
      key = "splitevery",
      injectionKey = "SPLIT_EVERY",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.SPLIT_EVERY")
  private int splitEvery;

  /** Add the transform copy number to the filename. */
  @HopMetadataProperty(
      key = "split",
      injectionKey = "INC_TRANSFORMNR_IN_FILENAME",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.INC_TRANSFORMNR_IN_FILENAME")
  private boolean transformNrInFilename;

  /** Add the date (yyyyMMdd) to the filename. */
  @HopMetadataProperty(
      key = "add_date",
      injectionKey = "INC_DATE_IN_FILENAME",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.INC_DATE_IN_FILENAME")
  private boolean dateInFilename;

  /** Add the time (HHmmss) to the filename. */
  @HopMetadataProperty(
      key = "add_time",
      injectionKey = "INC_TIME_IN_FILENAME",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.INC_TIME_IN_FILENAME")
  private boolean timeInFilename;

  /** Wrap the destination file in a zip archive. */
  @HopMetadataProperty(
      key = "zipped",
      injectionKey = "ZIPPED",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.ZIPPED")
  private boolean zipped;

  /** Add the produced filename(s) to the pipeline result file list. */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKey = "ADD_TO_RESULT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.ADD_TO_RESULT")
  private boolean addToResultFilenames;

  /** Defer file creation until the first input row is received. */
  @HopMetadataProperty(
      key = "do_not_open_newfile_init",
      injectionKey = "DO_NOT_CREATE_FILE_AT_STARTUP",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DO_NOT_CREATE_FILE_AT_STARTUP")
  private boolean doNotOpenNewFileInit;

  /** Delete the output file at the end of the run if no rows were written. */
  @HopMetadataProperty(
      key = "do_not_create_empty_file",
      injectionKey = "DO_NOT_CREATE_EMPTY_FILE",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DO_NOT_CREATE_EMPTY_FILE")
  private boolean doNotCreateEmptyFile;

  /** Use a custom date-time pattern instead of the date/time flags above. */
  @HopMetadataProperty(
      key = "SpecifyFormat",
      injectionKey = "SPEFICY_FORMAT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.SPEFICY_FORMAT")
  private boolean specifyFormat;

  @HopMetadataProperty(
      key = "date_time_format",
      injectionKey = "DATE_FORMAT",
      injectionKeyDescription = "AdvancedXMLOutput.Injection.DATE_FORMAT")
  private String dateTimeFormat;

  public XmlFileOutputSupport() {
    fileName = "";
    extension = "xml";
    splitEvery = 0;
  }

  public XmlFileOutputSupport(XmlFileOutputSupport o) {
    this();
    this.fileName = o.fileName;
    this.extension = o.extension;
    this.splitEvery = o.splitEvery;
    this.transformNrInFilename = o.transformNrInFilename;
    this.dateInFilename = o.dateInFilename;
    this.timeInFilename = o.timeInFilename;
    this.zipped = o.zipped;
    this.addToResultFilenames = o.addToResultFilenames;
    this.doNotOpenNewFileInit = o.doNotOpenNewFileInit;
    this.doNotCreateEmptyFile = o.doNotCreateEmptyFile;
    this.specifyFormat = o.specifyFormat;
    this.dateTimeFormat = o.dateTimeFormat;
  }

  /**
   * Build the resolved filename for a particular copy/split combination.
   *
   * @param variables variable resolver
   * @param copyNr the transform copy number (only used when {@link #transformNrInFilename} is set)
   * @param splitNr the split sequence number (only used when {@link #splitEvery} > 0)
   * @param zipArchive when true and {@link #zipped} is set, returns the .zip archive name; when
   *     false, returns the inner file name with the configured extension.
   */
  public String buildFilename(IVariables variables, int copyNr, int splitNr, boolean zipArchive) {
    SimpleDateFormat daf = new SimpleDateFormat();
    DecimalFormat df = new DecimalFormat("00000");

    String filename = variables.resolve(fileName);
    String realExtension = variables.resolve(extension);
    Date now = new Date();

    if (specifyFormat && !Utils.isEmpty(dateTimeFormat)) {
      daf.applyPattern(dateTimeFormat);
      filename += daf.format(now);
    } else {
      if (dateInFilename) {
        daf.applyPattern("yyyyMMdd");
        filename += "_" + daf.format(now);
      }
      if (timeInFilename) {
        daf.applyPattern("HHmmss");
        filename += "_" + daf.format(now);
      }
    }

    if (transformNrInFilename) {
      filename += "_" + copyNr;
    }
    if (splitEvery > 0) {
      filename += "_" + df.format(splitNr + 1);
    }

    if (zipped) {
      if (zipArchive) {
        filename += ".zip";
      } else if (!Utils.isEmpty(realExtension)) {
        filename += "." + realExtension;
      }
    } else if (!Utils.isEmpty(realExtension)) {
      filename += "." + realExtension;
    }
    return filename;
  }

  /**
   * Returns the sibling XSD filename for the data file at {@code copyNr}/{@code splitNr}. Always
   * uses extension {@code xsd} regardless of the data file's extension and is never wrapped in a
   * zip archive (the XSD lives next to the archive, not inside it).
   */
  public String buildXsdFilename(IVariables variables, int copyNr, int splitNr) {
    String saveExt = extension;
    boolean saveZipped = zipped;
    try {
      this.zipped = false;
      this.extension = "xsd";
      return buildFilename(variables, copyNr, splitNr, false);
    } finally {
      this.extension = saveExt;
      this.zipped = saveZipped;
    }
  }

  /** Returns up to {@code nr} sample filenames for use in the dialog's "Show files" preview. */
  public String[] previewFilenames(IVariables variables) {
    int copies = transformNrInFilename ? 3 : 1;
    int splits = splitEvery != 0 ? 3 : 1;
    int nr = copies * splits;
    if (nr > 1) {
      nr++;
    }
    String[] fileNames = new String[nr];
    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int split = 0; split < splits; split++) {
        fileNames[i++] = buildFilename(variables, copy, split, false);
      }
    }
    if (i < nr) {
      fileNames[i] = "...";
    }
    return fileNames;
  }
}
