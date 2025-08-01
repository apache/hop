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

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringEvaluator;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetector;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetectorBuilder;
import org.apache.hop.pipeline.transforms.filemetadata.util.encoding.EncodingDetector;

public class FileMetadata extends BaseTransform<FileMetadataMeta, FileMetadataData> {

  private static final Class<?> PKG = FileMetadata.class;
  private Object[] r;
  private Charset defaultCharset = StandardCharsets.ISO_8859_1;
  private long limitRows;

  /**
   * The constructor should simply pass on its arguments to the parent class.
   *
   * @param transformMeta transform description
   * @param data transform data class
   * @param copyNr transform copy
   * @param pipelineMeta transformation description
   * @param pipeline transformation executing
   */
  public FileMetadata(
      TransformMeta transformMeta,
      FileMetadataMeta meta,
      FileMetadataData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    // get incoming row, getRow() potentially blocks waiting for more rows
    // returns null if no more rows expected
    // note: getRow must be called at least once, otherwise getInputRowMeta() returns null
    r = getRow();

    if (first) {
      first = false;
      // remember whether the transform is consuming a stream, or generating a row
      data.isReceivingInput =
          !getPipelineMeta().findPreviousTransforms(getTransformMeta()).isEmpty();

      // processing existing rows?
      if (data.isReceivingInput && getInputRowMeta() != null) {
        // clone the input row structure and place it in our data object
        data.outputRowMeta = getInputRowMeta().clone();
      }
      // generating a new one?
      else {
        // create a new one
        data.outputRowMeta = new RowMeta();
      }

      // use meta.getFields() to change it, so it reflects the output row structure
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // -------------------------------------------------------------------------------
    // processing each passing row
    // -------------------------------------------------------------------------------
    if (data.isReceivingInput) {

      // if no more rows are expected, indicate transform is finished and processRow() should not be
      // called again
      if (r == null) {
        setOutputDone();
        return false;
      }

      buildOutputRows();

      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
        logBasic("LineNr " + getLinesRead());
      }

      // indicate that processRow() should be called again
      return true;

    }
    // -------------------------------------------------------------------------------
    // generating a single row with the results
    // -------------------------------------------------------------------------------
    else {

      buildOutputRows();
      // we're done
      setOutputDone();
      return false;
    }
  }

  public String getOutputFileName(Object[] row) throws HopException {
    String filename = null;
    if (row == null) {
      filename = variables.resolve(meta.getFileName());
      if (filename == null) {
        throw new HopFileException(
            BaseMessages.getString(PKG, "FileMetadata.Exception.FileNameNotSet"));
      }
    } else {
      int fileNameFieldIndex = getInputRowMeta().indexOfValue(meta.getFilenameField());
      if (fileNameFieldIndex < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "FileMetadata.Exception.FileNameFieldNotFound", meta.getFilenameField()));
      }
      IValueMeta fileNameMeta = getInputRowMeta().getValueMeta(fileNameFieldIndex);
      filename = variables.resolve(fileNameMeta.getString(row[fileNameFieldIndex]));

      if (filename == null) {
        throw new HopFileException(
            BaseMessages.getString(PKG, "FileMetadata.Exception.FileNameNotSet"));
      }
    }

    return filename;
  }

  private void buildOutputRows() throws HopException {

    // which index does the next field go to
    int idx = data.isReceivingInput ? getInputRowMeta().size() : 0;

    // prepare an output row
    Object[] outputRow =
        data.isReceivingInput
            ? RowDataUtil.createResizedCopy(r, data.outputRowMeta.size())
            : RowDataUtil.allocateRowData(data.outputRowMeta.size());

    // get the configuration from the dialog
    String fileName =
        getOutputFileName((data.isReceivingInput && meta.isFilenameInField() ? r : null));

    // if the file does not exist, just send an empty row
    try {
      if (!HopVfs.fileExists(fileName, variables)) {
        putRow(data.outputRowMeta, outputRow);
        return;
      }
    } catch (HopFileException e) {
      throw new HopTransformException(e.getMessage(), e);
    }

    String strLimitRows = resolve(meta.getLimitRows());
    if (strLimitRows.trim().isEmpty()) {
      limitRows = 0;
    } else {
      limitRows = Long.parseLong(strLimitRows);
    }

    defaultCharset = Charset.forName(resolve(meta.getDefaultCharset()));

    ArrayList<Character> delimiterCandidates = new ArrayList<>(4);
    for (FileMetadataMeta.FMCandidate delimiterCandidate : meta.getDelimiterCandidates()) {
      String candidate = resolve(delimiterCandidate.getCandidate());
      if (candidate.length() == 0) {
        logBasic("Warning: file metadata transform ignores empty delimiter candidate");
      } else if (candidate.length() > 1) {
        logBasic(
            "Warning: file metadata transform ignores non-character delimiter candidate: "
                + candidate);
      } else {
        delimiterCandidates.add(candidate.charAt(0));
      }
    }

    ArrayList<Character> enclosureCandidates = new ArrayList<>(4);
    for (FileMetadataMeta.FMCandidate enclosureCandidate : meta.getEnclosureCandidates()) {
      String candidate = resolve(enclosureCandidate.getCandidate());
      if (candidate.length() == 0) {
        logBasic("Warning: file metadata transform ignores empty enclosure candidate");
      } else if (candidate.length() > 1) {
        logBasic(
            "Warning: file metadata transform ignores non-character enclosure candidate: "
                + candidate);
      } else {
        enclosureCandidates.add(candidate.charAt(0));
      }
    }

    // guess the charset
    Charset detectedCharset = detectCharset(fileName);
    outputRow[idx++] = detectedCharset;

    // guess the delimiters
    DelimiterDetector.DetectionResult delimiters =
        detectDelimiters(fileName, detectedCharset, delimiterCandidates, enclosureCandidates);

    if (delimiters == null) {
      throw new HopTransformException(
          "Could not determine a consistent format for file " + fileName);
    }

    // delimiter
    outputRow[idx++] = delimiters.getDelimiter();
    // enclosure
    outputRow[idx++] =
        delimiters.getEnclosure() == null ? "" : delimiters.getEnclosure().toString();
    // field count = delimiter frequency on data lines +1
    outputRow[idx++] = delimiters.getDataLineFrequency() + 1L;
    // bad headers
    outputRow[idx++] = delimiters.getBadHeaders();
    // bad footers
    outputRow[idx++] = delimiters.getBadFooters();

    char delimiter = delimiters.getDelimiter();
    char enclosure = delimiters.getEnclosure() == null ? '\u0000' : delimiters.getEnclosure();
    long skipLines = delimiters.getBadHeaders();
    long dataLines = delimiters.getDataLines();

    try (BufferedReader inputReader =
        new BufferedReader(
            new InputStreamReader(HopVfs.getInputStream(fileName, variables), detectedCharset))) {
      while (skipLines > 0) {
        skipLines--;
        // Skip the line. There is no need to use a variable here.
        inputReader.readLine();
      }

      CSVParser csvParser =
          new CSVParserBuilder().withSeparator(delimiter).withQuoteChar(enclosure).build();

      try (CSVReader csvReader =
          new CSVReaderBuilder(inputReader).withCSVParser(csvParser).build()) {
        String[] firstLine = csvReader.readNext();
        dataLines--;

        StringEvaluator[] evaluators = new StringEvaluator[firstLine.length];
        for (int i = 0; i < evaluators.length; i++) {
          evaluators[i] = new StringEvaluator(true);
        }

        while (dataLines > 0) {
          dataLines--;
          String[] fields = csvReader.readNext();
          if (fields == null) break;
          for (int i = 0; i < fields.length; i++) {
            if (i < evaluators.length) evaluators[i].evaluateString(fields[i]);
          }
        }

        // find evaluation results, excluding and including the first line
        IValueMeta[] fields = new IValueMeta[evaluators.length];
        IValueMeta[] firstLineFields = new IValueMeta[evaluators.length];

        for (int i = 0; i < evaluators.length; i++) {
          fields[i] = evaluators[i].getAdvicedResult().getConversionMeta();
          evaluators[i].evaluateString(firstLine[i]);
          firstLineFields[i] = evaluators[i].getAdvicedResult().getConversionMeta();
        }

        // check whether to use the first line as a header, if there is a single type mismatch ->
        // yes
        // if all fields are strings -> yes
        boolean hasHeader = false;
        boolean allStrings = true;
        for (int i = 0; i < evaluators.length; i++) {

          if (fields[i].getType() != IValueMeta.TYPE_STRING) {
            allStrings = false;
          }

          if (fields[i].getType() != firstLineFields[i].getType()) {
            hasHeader = true;
            break;
          }
        }

        hasHeader = hasHeader || allStrings;

        if (hasHeader) {
          for (int i = 0; i < evaluators.length; i++) {
            fields[i].setName(firstLine[i].trim());
          }
        } else {
          // use the meta from the entire column
          fields = firstLineFields;
          int colNum = 1;
          for (int i = 0; i < evaluators.length; i++) {
            fields[i].setName("field_" + (colNum++));
          }
        }

        outputRow[idx++] = hasHeader;

        int fieldIdx = idx;
        for (int i = 0; i < evaluators.length; i++) {

          outputRow = RowDataUtil.createResizedCopy(outputRow, outputRow.length);

          idx = fieldIdx;
          outputRow[idx++] = fields[i].getName();
          outputRow[idx++] = fields[i].getTypeDesc();
          outputRow[idx++] = (fields[i].getLength() >= 0) ? (long) fields[i].getLength() : null;
          outputRow[idx++] =
              (fields[i].getPrecision() >= 0) ? (long) fields[i].getPrecision() : null;
          outputRow[idx++] = fields[i].getConversionMask();
          outputRow[idx++] = fields[i].getDecimalSymbol();
          outputRow[idx] = fields[i].getGroupingSymbol();

          putRow(data.outputRowMeta, outputRow);
        }
      }

    } catch (IOException | HopFileException e) {
      logError("IO Error while reading file: " + fileName + ". Invalid charset?");
      throw new HopTransformException(e.getMessage(), e);

    } catch (ArrayIndexOutOfBoundsException e) {
      logError("Error determining field types for: " + fileName + ". Inconsistent delimiters?");
      throw new HopTransformException(e.getMessage(), e);
    } catch (CsvValidationException e) {
      logError("Error validating CSV file " + fileName, e);
      throw new HopTransformException(e.getMessage(), e);
    }
  }

  private Charset detectCharset(String fileName) {
    try (InputStream stream = HopVfs.getInputStream(fileName, variables)) {
      return EncodingDetector.detectEncoding(
          stream, defaultCharset, limitRows * 500); // estimate a row is ~500 chars
    } catch (FileNotFoundException e) {
      throw new RuntimeException("File not found: " + fileName, e);
    } catch (IOException | HopFileException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private DelimiterDetector.DetectionResult detectDelimiters(
      String fileName,
      Charset charset,
      ArrayList<Character> delimiterCandidates,
      ArrayList<Character> enclosureCandidates) {

    // guess the delimiters

    try (BufferedReader f =
        new BufferedReader(
            new InputStreamReader(HopVfs.getInputStream(fileName, variables), charset))) {

      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(delimiterCandidates)
              .withEnclosureCandidates(enclosureCandidates)
              .withInput(f)
              .withLogger(getLogChannel())
              .withRowLimit(limitRows)
              .build();

      return detector.detectDelimiters();

    } catch (IOException | HopFileException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
