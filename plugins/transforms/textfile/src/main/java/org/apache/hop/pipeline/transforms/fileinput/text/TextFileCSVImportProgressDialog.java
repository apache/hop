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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

  private String doScan(IProgressMonitor monitor) throws HopException {
    return doScan(monitor, true);
  }

  private String doScan(IProgressMonitor monitor, final boolean failOnParseError)
      throws HopException {
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

    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields(outputRowMeta, null, null, null, variables, null);

    // Remove the storage meta-data (don't go for lazy conversion during scan)
    for (IValueMeta valueMeta : outputRowMeta.getValueMetaList()) {
      valueMeta.setStorageMetadata(null);
      valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    }

    IRowMeta convertRowMeta = outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

    // How many null values?
    int[] nullCounts = new int[nrFields]; // How many times null value?

    // String info
    String[] minStrings = new String[nrFields]; // min string
    String[] maxStrings = new String[nrFields]; // max string

    // Date info
    int[] dateFormatCount = new int[nrFields]; // How many date formats work?
    boolean[][] dateFormat =
        new boolean[nrFields][Const.getDateFormats().length]; // What are the date formats that
    // work?
    Date[][] minDate = new Date[nrFields][Const.getDateFormats().length]; // min date value
    Date[][] maxDate = new Date[nrFields][Const.getDateFormats().length]; // max date value

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

      nullCounts[i] = 0;
      minStrings[i] = "";
      maxStrings[i] = "";

      // Init data guess
      for (int j = 0; j < Const.getDateFormats().length; j++) {
        dateFormat[i][j] = true;
        minDate[i][j] = Const.MAX_DATE;
        maxDate[i][j] = Const.MIN_DATE;
      }
      dateFormatCount[i] = Const.getDateFormats().length;
    }

    ICsvInputAwareMeta<T> inputMeta = meta.clone();
    for (int i = 0; i < nrFields; i++) {
      inputMeta.getInputFields().get(i).setType(IValueMeta.TYPE_STRING);
    }

    // Sample <samples> rows...
    debug = "get first line";

    StringBuilder lineBuffer = new StringBuilder(256);
    int fileFormatType = meta.getFileFormatTypeNr();

    // If the file has a header we overwrite the first line
    // However, if it doesn't have a header, take a new line
    //

    line =
        TextFileLineUtil.getLine(
            log,
            reader,
            encodingType,
            fileFormatType,
            lineBuffer,
            meta.getEnclosure(),
            meta.getEscapeCharacter(),
            meta.isBreakInEnclosureAllowed());
    fileLineNumber++;

    if (meta.hasHeader()) {
      int skipped = 0;
      while (line != null && skipped < meta.getNrHeaderLines()) {
        line =
            TextFileLineUtil.getLine(
                log,
                reader,
                encodingType,
                fileFormatType,
                lineBuffer,
                meta.getEnclosure(),
                meta.getEscapeCharacter(),
                meta.isBreakInEnclosureAllowed());
        skipped++;
        fileLineNumber++;
      }
    }
    int linenr = 1;

    List<StringEvaluator> evaluators = new ArrayList<>();

    // Allocate number and date parsers
    DecimalFormat df2 = (DecimalFormat) NumberFormat.getInstance();
    DecimalFormatSymbols dfs2 = new DecimalFormatSymbols();
    SimpleDateFormat daf2 = new SimpleDateFormat();

    boolean errorFound = false;
    while (!errorFound
        && line != null
        && (linenr <= samples || samples == 0)
        && !monitor.isCanceled()) {
      monitor.subTask(
          BaseMessages.getString(
              PKG, "TextFileCSVImportProgressDialog.Task.ScanningLine", "" + linenr));
      if (samples > 0) {
        monitor.worked(1);
      }

      if (log.isDebug()) {
        debug = "convert line #" + linenr + " to row";
      }
      IRowMeta rowMeta = new RowMeta();
      meta.getFields(rowMeta, "transformName", null, null, variables, null);
      // Remove the storage meta-data (don't go for lazy conversion during scan)
      for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
        valueMeta.setStorageMetadata(null);
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }

      String delimiter = variables.resolve(meta.getDelimiter());
      String enclosure = variables.resolve(meta.getEnclosure());
      String escapeCharacter = variables.resolve(meta.getEscapeCharacter());
      Object[] r =
          TextFileInputUtils.convertLineToRow(
              log,
              new TextFileLine(line, fileLineNumber, null),
              inputMeta,
              null,
              0,
              outputRowMeta,
              convertRowMeta,
              FileInputList.createFilePathList(variables, meta.getInputFiles())[0],
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

        String string = getStringFromRow(rowMeta, r, i, failOnParseError);
        evaluator.evaluateString(string);
      }

      fileLineNumber++;
      linenr++;

      // Grab another line...
      //
      line =
          TextFileLineUtil.getLine(
              log,
              reader,
              encodingType,
              fileFormatType,
              lineBuffer,
              meta.getEnclosure(),
              meta.getEscapeCharacter(),
              meta.isBreakInEnclosureAllowed());
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
      StringEvaluator evaluator = evaluators.get(i);
      List<StringEvaluationResult> evaluationResults = evaluator.getStringEvaluationResults();

      // If we didn't find any matching result, it's a String...
      //
      if (evaluationResults.isEmpty()) {
        field.setType(IValueMeta.TYPE_STRING);
        field.setLength(evaluator.getMaxLength());
      } else {
        StringEvaluationResult result = evaluator.getAdvicedResult();
        if (result != null) {
          // Take the first option we find, list the others below...
          //
          IValueMeta conversionMeta = result.getConversionMeta();
          field.setType(conversionMeta.getType());
          field.setTrimType(conversionMeta.getTrimType());
          field.setFormat(conversionMeta.getConversionMask());
          field.setDecimalSymbol(conversionMeta.getDecimalSymbol());
          field.setGroupSymbol(conversionMeta.getGroupingSymbol());
          field.setLength(conversionMeta.getLength());
          field.setPrecision(conversionMeta.getPrecision());

          nullCounts[i] = result.getNrNull();
          minStrings[i] = result.getMin() == null ? "" : result.getMin().toString();
          maxStrings[i] = result.getMax() == null ? "" : result.getMax().toString();
        }
      }

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
                  BaseMessages.getString(
                      PKG, "TextFileCSVImportProgressDialog.Info.WarnNumberFormat"));
            }

            for (StringEvaluationResult seResult : evaluationResults) {
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
                      seResult.getMin()));
              resultsMessage.append(
                  BaseMessages.getString(
                      PKG,
                      "TextFileCSVImportProgressDialog.Info.NumberMaxValue",
                      seResult.getMax()));

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
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.NumberNrNullValues",
                  "" + nullCounts[i]));
          break;
        case IValueMeta.TYPE_STRING:
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.StringMaxLength",
                  "" + field.getLength()));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.StringMinValue", minStrings[i]));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG, "TextFileCSVImportProgressDialog.Info.StringMaxValue", maxStrings[i]));
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.StringNrNullValues",
                  "" + nullCounts[i]));
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
          if (dateFormatCount[i] > 1) {
            resultsMessage.append(
                BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Info.WarnDateFormat"));
          }
          if (!Utils.isEmpty(minStrings[i])) {
            for (int x = 0; x < Const.getDateFormats().length; x++) {
              if (dateFormat[i][x]) {
                resultsMessage.append(
                    BaseMessages.getString(
                        PKG,
                        "TextFileCSVImportProgressDialog.Info.DateFormat2",
                        Const.getDateFormats()[x]));
                Date mindate = minDate[i][x];
                Date maxdate = maxDate[i][x];
                resultsMessage.append(
                    BaseMessages.getString(
                        PKG,
                        "TextFileCSVImportProgressDialog.Info.DateMinValue",
                        mindate.toString()));
                resultsMessage.append(
                    BaseMessages.getString(
                        PKG,
                        "TextFileCSVImportProgressDialog.Info.DateMaxValue",
                        maxdate.toString()));

                daf2.applyPattern(Const.getDateFormats()[x]);
                try {
                  Date md = daf2.parse(minStrings[i]);
                  resultsMessage.append(
                      BaseMessages.getString(
                          PKG,
                          "TextFileCSVImportProgressDialog.Info.DateExample",
                          Const.getDateFormats()[x],
                          minStrings[i],
                          md.toString()));
                } catch (Exception e) {
                  if (log.isDetailed()) {
                    log.logDetailed(
                        "This is unexpected: parsing ["
                            + minStrings[i]
                            + "] with format ["
                            + Const.getDateFormats()[x]
                            + "] did not work.");
                  }
                }
              }
            }
          }
          resultsMessage.append(
              BaseMessages.getString(
                  PKG,
                  "TextFileCSVImportProgressDialog.Info.DateNrNullValues",
                  "" + nullCounts[i]));
          break;
        default:
          break;
      }
      if (nullCounts[i] == linenr - 1) {
        resultsMessage.append(
            BaseMessages.getString(PKG, "TextFileCSVImportProgressDialog.Info.AllNullValues"));
      }
      resultsMessage.append(Const.CR);
    }

    monitor.worked(1);
    monitor.done();

    return resultsMessage.toString();
  }
}
