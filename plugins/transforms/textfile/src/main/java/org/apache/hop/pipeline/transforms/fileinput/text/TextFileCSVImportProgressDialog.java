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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringEvaluationResult;
import org.apache.hop.core.util.StringEvaluator;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalField;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.pipeline.transform.common.ICsvInputAwareImportProgressDialog;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Takes care of displaying a dialog that will handle the wait while we're finding out what tables, views etc we can
 * reach in the database.
 *
 * @author Matt
 * @since 07-apr-2005
 */
public class TextFileCSVImportProgressDialog implements ICsvInputAwareImportProgressDialog {
  private static final Class<?> PKG = TextFileInputMeta.class; // For Translator

  private Shell shell;

  private final IVariables variables;
  private TextFileInputMeta meta;

  private int samples;

  private boolean replaceMeta;

  private String message;

  private String debug;

  private long rownumber;

  private InputStreamReader reader;

  private PipelineMeta pipelineMeta;

  private ILogChannel log;

  private EncodingType encodingType;

  /**
   * Creates a new dialog that will handle the wait while we're finding out what tables, views etc we can reach in the
   * database.
   */
  public TextFileCSVImportProgressDialog( Shell shell, IVariables variables, TextFileInputMeta meta, PipelineMeta pipelineMeta,
                                          InputStreamReader reader, int samples, boolean replaceMeta ) {
    this.shell = shell;
    this.variables = variables;
    this.meta = meta;
    this.reader = reader;
    this.samples = samples;
    this.replaceMeta = replaceMeta;
    this.pipelineMeta = pipelineMeta;

    message = null;
    debug = "init";
    rownumber = 1L;

    this.log = new LogChannel( pipelineMeta );

    this.encodingType = EncodingType.guessEncodingType( reader.getEncoding() );

  }

  public String open() {
    return open( true );
  }

  /**
   * @param failOnParseError if set to true, parsing failure on any line will cause parsing to be terminated; when
   *                         set to false, parsing failure on a given line will not prevent remaining lines from
   *                         being parsed - this allows us to analyze fields, even if some field is mis-configured
   *                         and causes a parsing error for the values of that field.
   */
  @Override
  public String open( final boolean failOnParseError ) {
    IRunnableWithProgress op = monitor -> {
      try {
        message = doScan( monitor, failOnParseError );
      } catch ( Exception e ) {
        e.printStackTrace();
        throw new InvocationTargetException( e, BaseMessages.getString( PKG,
          "TextFileCSVImportProgressDialog.Exception.ErrorScanningFile", "" + rownumber, debug, e.toString() ) );
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Title" ),
        BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Message" ), e );
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Title" ),
        BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.ErrorScanningFile.Message" ), e );
    }

    return message;
  }

  private String doScan( IProgressMonitor monitor ) throws HopException {
    return doScan( monitor, true );
  }

  private String doScan( IProgressMonitor monitor, final boolean failOnParseError ) throws HopException {
    if ( samples > 0 ) {
      monitor.beginTask( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Task.ScanningFile" ), samples
        + 1 );
    } else {
      monitor.beginTask( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Task.ScanningFile" ), 2 );
    }

    String line = "";
    long fileLineNumber = 0;

    DecimalFormatSymbols dfs = new DecimalFormatSymbols();

    int nrFields = meta.inputFields.length;

    IRowMeta outputRowMeta = new RowMeta();
    meta.getFields( outputRowMeta, null, null, null, variables, null );

    // Remove the storage meta-data (don't go for lazy conversion during scan)
    for ( IValueMeta valueMeta : outputRowMeta.getValueMetaList() ) {
      valueMeta.setStorageMetadata( null );
      valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    }

    IRowMeta convertRowMeta = outputRowMeta.cloneToType( IValueMeta.TYPE_STRING );

    // How many null values?
    int[] nrnull = new int[ nrFields ]; // How many times null value?

    // String info
    String[] minstr = new String[ nrFields ]; // min string
    String[] maxstr = new String[ nrFields ]; // max string
    boolean[] firststr = new boolean[ nrFields ]; // first occ. of string?

    // Date info
    boolean[] isDate = new boolean[ nrFields ]; // is the field perhaps a Date?
    int[] dateFormatCount = new int[ nrFields ]; // How many date formats work?
    boolean[][] dateFormat = new boolean[ nrFields ][ Const.getDateFormats().length ]; // What are the date formats that
    // work?
    Date[][] minDate = new Date[ nrFields ][ Const.getDateFormats().length ]; // min date value
    Date[][] maxDate = new Date[ nrFields ][ Const.getDateFormats().length ]; // max date value

    // Number info
    boolean[] isNumber = new boolean[ nrFields ]; // is the field perhaps a Number?
    int[] numberFormatCount = new int[ nrFields ]; // How many number formats work?
    boolean[][] numberFormat = new boolean[ nrFields ][ Const.getNumberFormats().length ]; // What are the number format
    // that work?
    double[][] minValue = new double[ nrFields ][ Const.getDateFormats().length ]; // min number value
    double[][] maxValue = new double[ nrFields ][ Const.getDateFormats().length ]; // max number value
    int[][] numberPrecision = new int[ nrFields ][ Const.getNumberFormats().length ]; // remember the precision?
    int[][] numberLength = new int[ nrFields ][ Const.getNumberFormats().length ]; // remember the length?

    for ( int i = 0; i < nrFields; i++ ) {
      BaseFileField field = meta.inputFields[ i ];

      if ( log.isDebug() ) {
        debug = "init field #" + i;
      }

      if ( replaceMeta ) { // Clear previous info...

        field.setName( meta.inputFields[ i ].getName() );
        field.setType( meta.inputFields[ i ].getType() );
        field.setFormat( "" );
        field.setLength( -1 );
        field.setPrecision( -1 );
        field.setCurrencySymbol( dfs.getCurrencySymbol() );
        field.setDecimalSymbol( "" + dfs.getDecimalSeparator() );
        field.setGroupSymbol( "" + dfs.getGroupingSeparator() );
        field.setNullString( "-" );
        field.setTrimType( IValueMeta.TRIM_TYPE_NONE );
      }

      nrnull[ i ] = 0;
      minstr[ i ] = "";
      maxstr[ i ] = "";
      firststr[ i ] = true;

      // Init data guess
      isDate[ i ] = true;
      for ( int j = 0; j < Const.getDateFormats().length; j++ ) {
        dateFormat[ i ][ j ] = true;
        minDate[ i ][ j ] = Const.MAX_DATE;
        maxDate[ i ][ j ] = Const.MIN_DATE;
      }
      dateFormatCount[ i ] = Const.getDateFormats().length;

      // Init number guess
      isNumber[ i ] = true;
      for ( int j = 0; j < Const.getNumberFormats().length; j++ ) {
        numberFormat[ i ][ j ] = true;
        minValue[ i ][ j ] = Double.MAX_VALUE;
        maxValue[ i ][ j ] = -Double.MAX_VALUE;
        numberPrecision[ i ][ j ] = -1;
        numberLength[ i ][ j ] = -1;
      }
      numberFormatCount[ i ] = Const.getNumberFormats().length;
    }

    TextFileInputMeta strinfo = (TextFileInputMeta) meta.clone();
    for ( int i = 0; i < nrFields; i++ ) {
      strinfo.inputFields[ i ].setType( IValueMeta.TYPE_STRING );
    }

    // Sample <samples> rows...
    debug = "get first line";

    StringBuilder lineBuffer = new StringBuilder( 256 );
    int fileFormatType = meta.getFileFormatTypeNr();

    // If the file has a header we overwrite the first line
    // However, if it doesn't have a header, take a new line
    //

    line = TextFileLineUtil.getLine( log, reader, encodingType, fileFormatType, lineBuffer );
    fileLineNumber++;

    if ( meta.content.header ) {
      int skipped = 0;
      while ( line != null && skipped < meta.content.nrHeaderLines ) {
        line = TextFileLineUtil.getLine( log, reader, encodingType, fileFormatType, lineBuffer );
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
    while ( !errorFound && line != null && ( linenr <= samples || samples == 0 ) && !monitor.isCanceled() ) {
      monitor.subTask( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Task.ScanningLine", ""
        + linenr ) );
      if ( samples > 0 ) {
        monitor.worked( 1 );
      }

      if ( log.isDebug() ) {
        debug = "convert line #" + linenr + " to row";
      }
      IRowMeta rowMeta = new RowMeta();
      meta.getFields( rowMeta, "transformName", null, null, variables, null );
      // Remove the storage meta-data (don't go for lazy conversion during scan)
      for ( IValueMeta valueMeta : rowMeta.getValueMetaList() ) {
        valueMeta.setStorageMetadata( null );
        valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
      }

      String delimiter = variables.resolve( meta.content.separator );
      String enclosure = variables.resolve( meta.content.enclosure );
      String escapeCharacter = variables.resolve( meta.content.escapeCharacter );
      Object[] r =
        TextFileInputUtils.convertLineToRow( log, new TextFileLine( line, fileLineNumber, null ), strinfo, null, 0,
          outputRowMeta, convertRowMeta, FileInputList.createFilePathList( variables, meta.inputFiles.fileName,
            meta.inputFiles.fileMask, meta.inputFiles.excludeFileMask, meta.inputFiles.fileRequired, meta
              .inputFiles.includeSubFolderBoolean() )[ 0 ], rownumber, delimiter, enclosure, escapeCharacter, null,
          new BaseFileInputAdditionalField(), null, null, false, null, null, null, null, null, failOnParseError );

      if ( r == null ) {
        errorFound = true;
        continue;
      }
      rownumber++;
      for ( int i = 0; i < nrFields && i < r.length; i++ ) {
        StringEvaluator evaluator;
        if ( i >= evaluators.size() ) {
          evaluator = new StringEvaluator( true );
          evaluators.add( evaluator );
        } else {
          evaluator = evaluators.get( i );
        }

        String string = getStringFromRow( rowMeta, r, i, failOnParseError );

        if ( i == 0 ) {
          System.out.println();
        }
        evaluator.evaluateString( string );
      }

      fileLineNumber++;
      if ( r != null ) {
        linenr++;
      }

      // Grab another line...
      //
      line = TextFileLineUtil.getLine( log, reader, encodingType, fileFormatType, lineBuffer );
    }

    monitor.worked( 1 );
    monitor.setTaskName( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Task.AnalyzingResults" ) );

    // Show information on items using a dialog box
    //
    StringBuilder message = new StringBuilder();
    message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.ResultAfterScanning", ""
      + ( linenr - 1 ) ) );
    message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.HorizontalLine" ) );

    for ( int i = 0; i < nrFields; i++ ) {
      BaseFileField field = meta.inputFields[ i ];
      StringEvaluator evaluator = evaluators.get( i );
      List<StringEvaluationResult> evaluationResults = evaluator.getStringEvaluationResults();

      // If we didn't find any matching result, it's a String...
      //
      if ( evaluationResults.isEmpty() ) {
        field.setType( IValueMeta.TYPE_STRING );
        field.setLength( evaluator.getMaxLength() );
      } else {
        StringEvaluationResult result = evaluator.getAdvicedResult();
        if ( result != null ) {
          // Take the first option we find, list the others below...
          //
          IValueMeta conversionMeta = result.getConversionMeta();
          field.setType( conversionMeta.getType() );
          field.setTrimType( conversionMeta.getTrimType() );
          field.setFormat( conversionMeta.getConversionMask() );
          field.setDecimalSymbol( conversionMeta.getDecimalSymbol() );
          field.setGroupSymbol( conversionMeta.getGroupingSymbol() );
          field.setLength( conversionMeta.getLength() );
          field.setPrecision( conversionMeta.getPrecision() );

          nrnull[ i ] = result.getNrNull();
          minstr[ i ] = result.getMin() == null ? "" : result.getMin().toString();
          maxstr[ i ] = result.getMax() == null ? "" : result.getMax().toString();
        }
      }

      message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.FieldNumber", "" + ( i
        + 1 ) ) );

      message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.FieldName", field
        .getName() ) );
      message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.FieldType", field
        .getTypeDesc() ) );

      switch ( field.getType() ) {
        case IValueMeta.TYPE_NUMBER:
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.EstimatedLength", ( field
            .getLength() < 0 ? "-" : "" + field.getLength() ) ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.EstimatedPrecision", field
            .getPrecision() < 0 ? "-" : "" + field.getPrecision() ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberFormat", field
            .getFormat() ) );

          if ( !evaluationResults.isEmpty() ) {
            if ( evaluationResults.size() > 1 ) {
              message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.WarnNumberFormat" ) );
            }

            for ( StringEvaluationResult seResult : evaluationResults ) {
              String mask = seResult.getConversionMeta().getConversionMask();

              message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberFormat2",
                mask ) );
              message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.TrimType", seResult
                .getConversionMeta().getTrimType() ) );
              message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberMinValue",
                seResult.getMin() ) );
              message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberMaxValue",
                seResult.getMax() ) );

              try {
                df2.applyPattern( mask );
                df2.setDecimalFormatSymbols( dfs2 );
                double mn = df2.parse( seResult.getMin().toString() ).doubleValue();
                message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberExample", mask,
                  seResult.getMin(), Double.toString( mn ) ) );
              } catch ( Exception e ) {
                if ( log.isDetailed() ) {
                  log.logDetailed( "This is unexpected: parsing [" + seResult.getMin() + "] with format [" + mask
                    + "] did not work." );
                }
              }
            }
          }
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.NumberNrNullValues", ""
            + nrnull[ i ] ) );
          break;
        case IValueMeta.TYPE_STRING:
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.StringMaxLength", ""
            + field.getLength() ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.StringMinValue",
            minstr[ i ] ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.StringMaxValue",
            maxstr[ i ] ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.StringNrNullValues", ""
            + nrnull[ i ] ) );
          break;
        case IValueMeta.TYPE_DATE:
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateMaxLength", field
            .getLength() < 0 ? "-" : "" + field.getLength() ) );
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateFormat", field
            .getFormat() ) );
          if ( dateFormatCount[ i ] > 1 ) {
            message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.WarnDateFormat" ) );
          }
          if ( !Utils.isEmpty( minstr[ i ] ) ) {
            for ( int x = 0; x < Const.getDateFormats().length; x++ ) {
              if ( dateFormat[ i ][ x ] ) {
                message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateFormat2", Const
                  .getDateFormats()[ x ] ) );
                Date mindate = minDate[ i ][ x ];
                Date maxdate = maxDate[ i ][ x ];
                message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateMinValue",
                  mindate.toString() ) );
                message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateMaxValue",
                  maxdate.toString() ) );

                daf2.applyPattern( Const.getDateFormats()[ x ] );
                try {
                  Date md = daf2.parse( minstr[ i ] );
                  message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateExample", Const
                    .getDateFormats()[ x ], minstr[ i ], md.toString() ) );
                } catch ( Exception e ) {
                  if ( log.isDetailed() ) {
                    log.logDetailed( "This is unexpected: parsing [" + minstr[ i ] + "] with format [" + Const
                      .getDateFormats()[ x ] + "] did not work." );
                  }
                }
              }
            }
          }
          message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.DateNrNullValues", ""
            + nrnull[ i ] ) );
          break;
        default:
          break;
      }
      if ( nrnull[ i ] == linenr - 1 ) {
        message.append( BaseMessages.getString( PKG, "TextFileCSVImportProgressDialog.Info.AllNullValues" ) );
      }
      message.append( Const.CR );

    }

    monitor.worked( 1 );
    monitor.done();

    return message.toString();

  }
}
