/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringEvaluationResult;
import org.apache.hop.core.util.StringEvaluator;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalFields;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.pipeline.transform.common.ICsvInputAwareImportProgressDialog;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.eclipse.swt.widgets.Shell;

/**
 * Takes care of displaying a dialog that will handle the wait while we're finding out what tables,
 * views etc. we can reach in the database.
 */
public class TextFileCSVImportProgressDialog<T extends ITextFileInputField>
    implements ICsvInputAwareImportProgressDialog {
  private static final Class<?> PKG = TextFileInputMeta.class;

  private final Shell shell;

  private final IVariables variables;
  private final ICsvInputAwareMeta<T> meta;

  private final int samples;

  private final boolean replaceMeta;

  private String message;

  private String debug;

  private long rowNumber;

  private final InputStreamReader reader;

  private final ILogChannel log;

  private final EncodingType encodingType;

  /**
   * Creates a new dialog that will handle the wait while we're finding out what tables, views etc.
   * we can reach in the database.
   */
  public TextFileCSVImportProgressDialog(
      Shell shell,
      IVariables variables,
      ICsvInputAwareMeta<T> meta,
      PipelineMeta pipelineMeta,
      InputStreamReader reader,
      int samples,
      boolean replaceMeta) {
    this.shell = shell;
    this.variables = variables;
    this.meta = meta;
    this.reader = reader;
    this.samples = samples;
    this.replaceMeta = replaceMeta;
    this.message = null;
    this.debug = "init";
    this.rowNumber = 1L;

    this.log = new LogChannel(pipelineMeta);

    this.encodingType = EncodingType.guessEncodingType(reader.getEncoding());
  }

  public String open() {
    return open(true);
  }

  /**
   * @param failOnParseError if set to true, parsing failure on any line will cause parsing to be
   *     terminated; when set to false, parsing failure on a given line will not prevent remaining
   *     lines from being parsed - this allows us to analyze fields, even if some field is
   *     mis-configured and causes a parsing error for the values of that field.
   */
  @Override
  public String open(final boolean failOnParseError) {
    IRunnableWithProgress op =
        monitor -> {
          try {
            message = doScan(monitor, failOnParseError);
          } catch (Exception e) {
            throw new InvocationTargetException(
                e,
                BaseMessages.getString(
                    PKG,
                    "TextFileCSVImportProgressDialog.Exception.ErrorScanningFile",
                    "" + rowNumber,
                    debug,
                    e.toString()));
          }
        };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(true, op);
    } catch (InvocationTargetException | InterruptedException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Title"),
          BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Message"),
          e);
    }

    return message;
  }

  /**
   * File name stored on each sampled row. Resolved once: {@link FileInputList#createFilePathList}
   * stats the file, and on a remote URL that is several HTTP calls. Doing it per sample line is
   * what made "Get fields" on a large CSV take forever.
   */
  protected String resolveSampleFileName() throws HopException {
    String[] paths = FileInputList.createFilePathList(variables, meta.getInputFiles());
    if (paths.length == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "TextFileInputDialog.NoValidFile.DialogMessage"));
    }
    return paths[0];
  }

  String doScan(IProgressMonitor monitor, final boolean failOnParseError) throws HopException {
    if (samples > 0) {
      monitor.beginTask(
          BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Task.ScanningFile"),
          samples + 1);
    } else {
      monitor.beginTask(
          BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Task.ScanningFile"), 2);
    }

    String line = "";
    long fileLineNumber = 0;

    DecimalFormatSymbols dfs = new DecimalFormatSymbols();

    int nrFields = meta.getInputFields().size();

    for (int i = 0; i < nrFields; i++) {
      T field = meta.getInputFields().get(i);
      if (replaceMeta) { // Clear previous info...
        field.setFormat("");
        field.setLength(-1);
        field.setPrecision(-1);
        field.setCurrencySymbol(dfs.getCurrencySymbol());
        field.setDecimalSymbol("" + dfs.getDecimalSeparator());
        field.setGroupSymbol("" + dfs.getGroupingSeparator());
        field.setNullString("-");
        field.setTrimType(IValueMeta.TRIM_TYPE_NONE);
      }
    }

    // Row layout once. Repeating getFields per line repeated a remote file listing when "prepend
    // file name" was on, and it does not change while we sample.
    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, "transformName", null, null, variables, null);
    for (IValueMeta valueMeta : outputRowMeta.getValueMetaList()) {
      valueMeta.setStorageMetadata(null);
      valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    }
    IRowMeta convertRowMeta = outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

    ICsvInputAwareMeta<T> inputMeta = meta.clone();
    for (int i = 0; i < nrFields; i++) {
      inputMeta.getInputFields().get(i).setType(IValueMeta.TYPE_STRING);
    }

    String sampleFileName = resolveSampleFileName();
    String delimiter = variables.resolve(meta.getDelimiter());
    String enclosure = variables.resolve(meta.getEnclosure());
    String escapeCharacter = variables.resolve(meta.getEscapeCharacter());

    // Sample <samples> rows...
    debug = "get first line";

    StringBuilder lineBuffer = new StringBuilder(256);
    int fileFormatType = meta.getFileFormatTypeNr();

    // If the file has a header we overwrite the first line
    // However, if it doesn't have a header, take a new line
    //

    line = readSampleLine(lineBuffer, fileFormatType);
    fileLineNumber++;

    if (meta.hasHeader()) {
      int skipped = 0;
      while (line != null && skipped < meta.getNrHeaderLines()) {
        line = readSampleLine(lineBuffer, fileFormatType);
        skipped++;
        fileLineNumber++;
      }
    }
    int linenr = 1;

    List<StringEvaluator> evaluators = new ArrayList<>();

    DecimalFormat df2 = (DecimalFormat) NumberFormat.getInstance();
    DecimalFormatSymbols dfs2 = new DecimalFormatSymbols();

    boolean errorFound = false;
    while (!errorFound
        && line != null
        && (linenr <= samples || samples == 0)
        && !monitor.isCanceled()) {
      // A blank line is not a sample row. Unix mode on a CRLF file produces one after every row;
      // counting it would make the first sample empty and burn the sample budget.
      if (meta.skipEmptyLines() && line.isEmpty()) {
        fileLineNumber++;
        line = readSampleLine(lineBuffer, fileFormatType);
        continue;
      }

      monitor.subTask(
          BaseMessages.getString(
              PKG, "TextFileCSVImportProgressDialog.Task.ScanningLine", "" + linenr));
      if (samples > 0) {
        monitor.worked(1);
      }

      if (log.isDebug()) {
        debug = "convert line #" + linenr + " to row";
      }
      Object[] r =
          TextFileInputUtils.convertLineToRow(
              log,
              new TextFileLine(line, fileLineNumber, null),
              inputMeta,
              null,
              0,
              outputRowMeta,
              convertRowMeta,
              sampleFileName,
              rowNumber,
              delimiter,
              enclosure,
              escapeCharacter,
              null,
              new BaseFileInputAdditionalFields(),
              null,
              null,
              false,
              null,
              null,
              null,
              null,
              null,
              failOnParseError);
      if (r == null) {
        errorFound = true;
        continue;
      }
      rowNumber++;
      for (int i = 0; i < nrFields && i < r.length; i++) {
        StringEvaluator evaluator;
        if (i >= evaluators.size()) {
          evaluator = new StringEvaluator(true);
          evaluators.add(evaluator);
        } else {
          evaluator = evaluators.get(i);
        }

        String string = getStringFromRow(outputRowMeta, r, i, failOnParseError);
        evaluator.evaluateString(string);
      }

      fileLineNumber++;
      linenr++;

      line = readSampleLine(lineBuffer, fileFormatType);
    }

    monitor.worked(1);
    monitor.setTaskName(
        BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Task.AnalyzingResults"));

    // Show information on items using a dialog box
    //
    StringBuilder resultsMessage = new StringBuilder();
    resultsMessage.append(
        BaseMessages.getString(
            PKG, "TextFileCSVImportProgressDialog.Info.ResultAfterScanning", "" + (linenr - 1)));
    resultsMessage.append(
        BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Info.HorizontalLine"));

    for (int i = 0; i < nrFields; i++) {
      T field = meta.getInputFields().get(i);
      StringEvaluator evaluator = i < evaluators.size() ? evaluators.get(i) : null;
      // Copy successful masks before getAdvicedResult(), which drops the ones it does not keep.
      List<StringEvaluationResult> evaluationResults =
          evaluator == null ? Collections.emptyList() : evaluator.getStringEvaluationResults();

      StringEvaluationResult advised = null;
      if (evaluator == null || evaluationResults.isEmpty()) {
        // Nothing converted: the column is a string. Still ask the evaluator for min/max, or the
        // summary prints an empty value under the field name.
        field.setType(IValueMeta.TYPE_STRING);
        field.setLength(evaluator == null ? -1 : evaluator.getMaxLength());
        if (evaluator != null) {
          advised = evaluator.getAdvicedResult();
        }
      } else {
        advised = evaluator.getAdvicedResult();
        if (advised != null) {
          IValueMeta conversionMeta = advised.getConversionMeta();
          field.setType(conversionMeta.getType());
          field.setTrimType(conversionMeta.getTrimType());
          field.setFormat(conversionMeta.getConversionMask());
          field.setDecimalSymbol(conversionMeta.getDecimalSymbol());
          field.setGroupSymbol(conversionMeta.getGroupingSymbol());
          field.setLength(conversionMeta.getLength());
          field.setPrecision(conversionMeta.getPrecision());
          // An integer guess leaves precision at -1. The fields grid hides -1, so Get Fields
          // would show no precision for a column that sampled as a whole number.
          if (field.getType() == IValueMeta.TYPE_INTEGER && field.getPrecision() < 0) {
            field.setPrecision(0);
          }
        }
      }

      String minValue = valueText(advised == null ? null : advised.getMin());
      String maxValue = valueText(advised == null ? null : advised.getMax());
      int nullCount = advised == null ? 0 : advised.getNrNull();

      resultsMessage.append(
          BaseMessages.getString(
              PKG, "TextFileCSVImportProgressDialog.Info.FieldNumber", "" + (i + 1)));
      resultsMessage.append(
          BaseMessages.getString(
              PKG, "TextFileCSVImportProgressDialog.Info.FieldName", field.getName()));
      resultsMessage.append(
          BaseMessages.getString(
              PKG, "TextFileCSVImportProgressDialog.Info.FieldType", field.getTypeDesc()));

      switch (field.getType()) {
        case IValueMeta.TYPE_NUMBER:
        case IValueMeta.TYPE_INTEGER:
          appendNumericSummary(resultsMessage, field, evaluationResults, nullCount, df2, dfs2);
          break;
        case IValueMeta.TYPE_STRING:
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.StringMaxLength",
                  "" + field.getLength()));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.StringMinValue", minValue));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.StringMaxValue", maxValue));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.StringNrNullValues", "" + nullCount));
          break;
        case IValueMeta.TYPE_DATE:
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.DateMaxLength",
                  field.getLength() < 0 ? "-" : "" + field.getLength()));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.DateFormat", field.getFormat()));
          for (StringEvaluationResult seResult : evaluationResults) {
            if (!seResult.getConversionMeta().isDate()) {
              continue;
            }
            resultsMessage.append(
                BaseMessages.getString(
                    PKG,
                    "TextFileCSVImportProgressDialog.Info.DateFormat2",
                    seResult.getConversionMeta().getConversionMask()));
            resultsMessage.append(
                BaseMessages.getString(
                    PKG,
                    "TextFileCSVImportProgressDialog.Info.DateMinValue",
                    valueText(seResult.getMin())));
            resultsMessage.append(
                BaseMessages.getString(
                    PKG,
                    "TextFileCSVImportProgressDialog.Info.DateMaxValue",
                    valueText(seResult.getMax())));
          }
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.DateNrNullValues", "" + nullCount));
          break;
        default:
          break;
      }
      if (nullCount > 0 && nullCount == linenr - 1) {
        resultsMessage.append(
            BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Info.AllNullValues"));
      }
      resultsMessage.append(Const.CR);
    }

    monitor.worked(1);
    // Do not call monitor.done() here. That disposes the progress dialog from inside this call,
    // and open() then returns before it can publish the result. The grid would keep the header
    // names and never receive the sampled types, lengths and masks. The dialog closes itself
    // after open() has stored the message.
    String result = resultsMessage.toString();
    message = result;
    return result;
  }

  private void appendNumericSummary(
      StringBuilder resultsMessage,
      T field,
      List<StringEvaluationResult> evaluationResults,
      int nullCount,
      DecimalFormat df2,
      DecimalFormatSymbols dfs2) {
    resultsMessage.append(
        BaseMessages.getString(
            PKG,
            "TextFileCSVImportProgressDialog.Info.EstimatedLength",
            (field.getLength() < 0 ? "-" : "" + field.getLength())));
    resultsMessage.append(
        BaseMessages.getString(
            PKG,
            "TextFileCSVImportProgressDialog.Info.EstimatedPrecision",
            field.getPrecision() < 0 ? "-" : "" + field.getPrecision()));
    resultsMessage.append(
        BaseMessages.getString(
            PKG, "TextFileCSVImportProgressDialog.Info.NumberFormat", field.getFormat()));

    if (!evaluationResults.isEmpty()) {
      if (evaluationResults.size() > 1) {
        resultsMessage.append(
            BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Info.WarnNumberFormat"));
      }
      for (StringEvaluationResult seResult : evaluationResults) {
        if (!seResult.getConversionMeta().isNumeric()) {
          continue;
        }
        String mask = seResult.getConversionMeta().getConversionMask();
        resultsMessage.append(
            BaseMessages.getString(
                PKG, "TextFileCSVImportProgressDialog.Info.NumberFormat2", mask));
        resultsMessage.append(
            BaseMessages.getString(
                PKG,
                "TextFileCSVImportProgressDialog.Info.TrimType",
                seResult.getConversionMeta().getTrimType()));
        resultsMessage.append(
            BaseMessages.getString(
                PKG,
                "TextFileCSVImportProgressDialog.Info.NumberMinValue",
                valueText(seResult.getMin())));
        resultsMessage.append(
            BaseMessages.getString(
                PKG,
                "TextFileCSVImportProgressDialog.Info.NumberMaxValue",
                valueText(seResult.getMax())));
        if (seResult.getMin() != null && !Utils.isEmpty(mask)) {
          try {
            df2.applyPattern(mask);
            df2.setDecimalFormatSymbols(dfs2);
            double mn = df2.parse(seResult.getMin().toString()).doubleValue();
            resultsMessage.append(
                BaseMessages.getString(
                    PKG,
                    "TextFileCSVImportProgressDialog.Info.NumberExample",
                    mask,
                    seResult.getMin(),
                    Double.toString(mn)));
          } catch (Exception e) {
            if (log.isDetailed()) {
              log.logDetailed(
                  "This is unexpected: parsing ["
                      + seResult.getMin()
                      + "] with format ["
                      + mask
                      + "] did not work.");
            }
          }
        }
      }
    }
    resultsMessage.append(
        BaseMessages.getString(
            PKG, "TextFileCSVImportProgressDialog.Info.NumberNrNullValues", "" + nullCount));
  }

  private static String valueText(Object value) {
    return value == null ? "" : value.toString();
  }

  private String readSampleLine(StringBuilder lineBuffer, int fileFormatType) throws HopException {
    return TextFileLineUtil.getLine(
        log,
        reader,
        encodingType,
        fileFormatType,
        lineBuffer,
        meta.getEnclosure(),
        meta.getEscapeCharacter(),
        meta.isBreakInEnclosureAllowed());
  }
}
