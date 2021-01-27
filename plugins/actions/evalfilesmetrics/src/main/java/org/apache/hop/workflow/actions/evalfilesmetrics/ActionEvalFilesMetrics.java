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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'evaluate files metrics' action.
 *
 * @author Samatar Hassan
 * @since 26-02-2010
 */

@Action(
  id = "EVAL_FILES_METRICS",
  name = "i18n::ActionEvalFilesMetrics.Name",
  description = "i18n::ActionEvalFilesMetrics.Description",
  image = "EvalFilesMetrics.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/evalfilesmetrics.html"
)
public class ActionEvalFilesMetrics extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionEvalFilesMetrics.class; // For Translator

  
  public static final int SUCCESS_NUMBER_CONDITION_EQUAL = 0;
  public static final int SUCCESS_NUMBER_CONDITION_DIFFERENT = 1;
  public static final int SUCCESS_NUMBER_CONDITION_SMALLER = 2;
  public static final int SUCCESS_NUMBER_CONDITION_SMALLER_EQUAL = 3;
  public static final int SUCCESS_NUMBER_CONDITION_GREATER = 4;
  public static final int SUCCESS_NUMBER_CONDITION_GREATER_EQUAL = 5;
  public static final int SUCCESS_NUMBER_CONDITION_BETWEEN = 6;
  public static final int SUCCESS_NUMBER_CONDITION_IN_LIST = 7;
  public static final int SUCCESS_NUMBER_CONDITION_NOT_IN_LIST = 8;

  public static final String[] successNumberConditionCode = new String[] {
		    "equal", "different", "smaller", "smallequal", "greater", "greaterequal", "between", "inlist", "notinlist" };
  
  public static final String[] successNumberConditionDesc = new String[] {
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenEqual.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenDifferent.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenSmallThan.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenSmallOrEqualThan.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenGreaterThan.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenGreaterOrEqualThan.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessBetween.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenInList.Label" ),
		    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessWhenNotInList.Label" ), };
  
  public static final BigDecimal ONE = new BigDecimal( 1 );

  public static final String[] IncludeSubFoldersDesc = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };
  public static final String[] IncludeSubFoldersCodes = new String[] { "N", "Y" };
  private static final String YES = "Y";
  private static final String NO = "N";

  public static final String[] scaleDesc = new String[] {
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.Bytes.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.KBytes.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.MBytes.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.GBytes.Label" ) };
  public static final String[] scaleCodes = new String[] { "bytes", "kbytes", "mbytes", "gbytes" };
  public static final int SCALE_BYTES = 0;
  public static final int SCALE_KBYTES = 1;
  public static final int SCALE_MBYTES = 2;
  public static final int SCALE_GBYTES = 3;

  public int scale;

  public static final String[] SourceFilesDesc = new String[] {
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SourceFiles.Files.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SourceFiles.FilenamesResult.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.SourceFiles.PreviousResult.Label" ), };
  public static final String[] SourceFilesCodes = new String[] { "files", "filenamesresult", "previousresult" };
  public static final int SOURCE_FILES_FILES = 0;
  public static final int SOURCE_FILES_FILENAMES_RESULT = 1;
  public static final int SOURCE_FILES_PREVIOUS_RESULT = 2;

  public int sourceFiles;

  public static final String[] EvaluationTypeDesc = new String[] {
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.EvaluationType.Size.Label" ),
    BaseMessages.getString( PKG, "JobEvalFilesMetrics.EvaluationType.Count.Label" ), };
  public static final String[] EvaluationTypeCodes = new String[] { "size", "count", };
  public static final int EVALUATE_TYPE_SIZE = 0;
  public static final int EVALUATE_TYPE_COUNT = 1;

  public int evaluationType;

  private String comparevalue;
  private String minvalue;
  private String maxvalue;
  private int successConditionType;

  private String resultFilenamesWildcard;

  public boolean argFromPrevious;

  private String[] sourceFileFolder;
  private String[] sourceWildcard;
  private String[] sourceIncludeSubfolders;

  private BigDecimal evaluationValue;
  private BigDecimal filesCount;
  private long nrErrors;

  private String ResultFieldFile;
  private String ResultFieldWildcard;
  private String ResultFieldIncludesubFolders;

  private BigDecimal compareValue;
  private BigDecimal minValue;
  private BigDecimal maxValue;

  public ActionEvalFilesMetrics( String n ) {
    super( n, "" );
    sourceFileFolder = null;
    sourceWildcard = null;
    sourceIncludeSubfolders = null;
    scale = SCALE_BYTES;
    sourceFiles = SOURCE_FILES_FILES;
    evaluationType = EVALUATE_TYPE_SIZE;
    successConditionType = SUCCESS_NUMBER_CONDITION_GREATER;
    resultFilenamesWildcard = null;
    ResultFieldFile = null;
    ResultFieldWildcard = null;
    ResultFieldIncludesubFolders = null;
  }

  public ActionEvalFilesMetrics() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    sourceFileFolder = new String[ nrFields ];
    sourceWildcard = new String[ nrFields ];
    sourceIncludeSubfolders = new String[ nrFields ];
  }

  public Object clone() {
    ActionEvalFilesMetrics je = (ActionEvalFilesMetrics) super.clone();
    if ( sourceFileFolder != null ) {
      int nrFields = sourceFileFolder.length;
      je.allocate( nrFields );
      System.arraycopy( sourceFileFolder, 0, je.sourceFileFolder, 0, nrFields );
      System.arraycopy( sourceWildcard, 0, je.sourceWildcard, 0, nrFields );
      System.arraycopy( sourceIncludeSubfolders, 0, je.sourceIncludeSubfolders, 0, nrFields );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXml() );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "result_filenames_wildcard", resultFilenamesWildcard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "Result_field_file", ResultFieldFile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "Result_field_wildcard", ResultFieldWildcard ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "Result_field_includesubfolders", ResultFieldIncludesubFolders ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( sourceFileFolder != null ) {
      for ( int i = 0; i < sourceFileFolder.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "source_filefolder", sourceFileFolder[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "wildcard", sourceWildcard[ i ] ) );
        retval
          .append( "          " ).append( XmlHandler.addTagValue( "include_subFolders", sourceIncludeSubfolders[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );
    retval.append( "      " ).append( XmlHandler.addTagValue( "comparevalue", comparevalue ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "minvalue", minvalue ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "maxvalue", maxvalue ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "successnumbercondition", getSuccessNumberConditionCode( successConditionType ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "source_files", getSourceFilesCode( sourceFiles ) ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "evaluation_type", getEvaluationTypeCode( evaluationType ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "scale", getScaleCode( scale ) ) );
    return retval.toString();
  }

  public static String getIncludeSubFolders( String tt ) {
    if ( tt == null ) {
      return IncludeSubFoldersCodes[ 0 ];
    }
    if ( tt.equals( IncludeSubFoldersDesc[ 1 ] ) ) {
      return IncludeSubFoldersCodes[ 1 ];
    } else {
      return IncludeSubFoldersCodes[ 0 ];
    }
  }

  public static String getIncludeSubFoldersDesc( String tt ) {
    if ( tt == null ) {
      return IncludeSubFoldersDesc[ 0 ];
    }
    if ( tt.equals( IncludeSubFoldersCodes[ 1 ] ) ) {
      return IncludeSubFoldersDesc[ 1 ];
    } else {
      return IncludeSubFoldersDesc[ 0 ];
    }
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        sourceFileFolder[ i ] = XmlHandler.getTagValue( fnode, "source_filefolder" );
        sourceWildcard[ i ] = XmlHandler.getTagValue( fnode, "wildcard" );
        sourceIncludeSubfolders[ i ] = XmlHandler.getTagValue( fnode, "include_subFolders" );
      }

      resultFilenamesWildcard = XmlHandler.getTagValue( entrynode, "result_filenames_wildcard" );
      ResultFieldFile = XmlHandler.getTagValue( entrynode, "result_field_file" );
      ResultFieldWildcard = XmlHandler.getTagValue( entrynode, "result_field_wildcard" );
      ResultFieldIncludesubFolders = XmlHandler.getTagValue( entrynode, "result_field_includesubfolders" );
      comparevalue = XmlHandler.getTagValue( entrynode, "comparevalue" );
      minvalue = XmlHandler.getTagValue( entrynode, "minvalue" );
      maxvalue = XmlHandler.getTagValue( entrynode, "maxvalue" );
      successConditionType = getSuccessNumberConditionByCode( Const.NVL( XmlHandler.getTagValue(
          entrynode, "successnumbercondition" ), "" ) );
      sourceFiles = getSourceFilesByCode( Const.NVL( XmlHandler.getTagValue( entrynode, "source_files" ), "" ) );
      evaluationType =
        getEvaluationTypeByCode( Const.NVL( XmlHandler.getTagValue( entrynode, "evaluation_type" ), "" ) );
      scale = getScaleByCode( Const.NVL( XmlHandler.getTagValue( entrynode, "scale" ), "" ) );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "JobEvalFilesMetrics.Error.Exception.UnableLoadXML" ), xe );
    }
  }

  public Result execute( Result previousResult, int nr ) throws HopException {
    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;


    try {
      initMetrics();
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Error.Init", e.toString() ) );
      return result;
    }

    // Get source and destination files, also wildcard
    String[] vSourceFileFolder = sourceFileFolder;
    String[] vwildcard = sourceWildcard;
    String[] vincludeSubFolders = sourceIncludeSubfolders;

    switch ( getSourceFiles() ) {
      case SOURCE_FILES_PREVIOUS_RESULT:
        // Filenames are retrieved from previous result rows

        String realResultFieldFile = resolve( getResultFieldFile() );
        String realResultFieldWildcard = resolve( getResultFieldWildcard() );
        String realResultFieldIncluseSubfolders = resolve( getResultFieldIncludeSubfolders() );

        int indexOfResultFieldFile = -1;
        if ( Utils.isEmpty( realResultFieldFile ) ) {
          logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Error.ResultFieldsFileMissing" ) );
          return result;
        }

        int indexOfResultFieldWildcard = -1;
        int indexOfResultFieldIncludeSubfolders = -1;

        // as such we must get rows
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobEvalFilesMetrics.Log.ArgFromPrevious.Found", ( rows != null ? rows.size() : 0 ) + "" ) );
        }

        if ( rows != null && rows.size() > 0 ) {
          // We get rows
          RowMetaAndData firstRow = rows.get( 0 );
          indexOfResultFieldFile = firstRow.getRowMeta().indexOfValue( realResultFieldFile );
          if ( indexOfResultFieldFile == -1 ) {
            logError( BaseMessages.getString(
              PKG, "JobEvalFilesMetrics.Error.CanNotFindField", realResultFieldFile ) );
            return result;
          }
          if ( !Utils.isEmpty( realResultFieldWildcard ) ) {
            indexOfResultFieldWildcard = firstRow.getRowMeta().indexOfValue( realResultFieldWildcard );
            if ( indexOfResultFieldWildcard == -1 ) {
              logError( BaseMessages.getString(
                PKG, "JobEvalFilesMetrics.Error.CanNotFindField", realResultFieldWildcard ) );
              return result;
            }
          }
          if ( !Utils.isEmpty( realResultFieldIncluseSubfolders ) ) {
            indexOfResultFieldIncludeSubfolders =
              firstRow.getRowMeta().indexOfValue( realResultFieldIncluseSubfolders );
            if ( indexOfResultFieldIncludeSubfolders == -1 ) {
              logError( BaseMessages.getString(
                PKG, "JobEvalFilesMetrics.Error.CanNotFindField", realResultFieldIncluseSubfolders ) );
              return result;
            }
          }

          for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {

            resultRow = rows.get( iteration );

            // Get source and destination file names, also wildcard
            String vSourceFileFolderPrevious = resultRow.getString( indexOfResultFieldFile, null );
            String vWildcardPrevious = null;
            if ( indexOfResultFieldWildcard > -1 ) {
              vWildcardPrevious = resultRow.getString( indexOfResultFieldWildcard, null );
            }
            String vincludeSubFoldersPrevious = NO;
            if ( indexOfResultFieldIncludeSubfolders > -1 ) {
              vincludeSubFoldersPrevious = resultRow.getString( indexOfResultFieldIncludeSubfolders, NO );
            }

            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEvalFilesMetrics.Log.ProcessingRow", vSourceFileFolderPrevious, vWildcardPrevious ) );
            }

            ProcessFileFolder(
              vSourceFileFolderPrevious, vWildcardPrevious, vincludeSubFoldersPrevious, parentWorkflow, result );
          }
        }

        break;
      case SOURCE_FILES_FILENAMES_RESULT:
        List<ResultFile> resultFiles = result.getResultFilesList();
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobEvalFilesMetrics.Log.ResultFilenames.Found",
            ( resultFiles != null ? resultFiles.size() : 0 ) + "" ) );
        }

        if ( resultFiles != null && resultFiles.size() > 0 ) {
          // Let's check wildcard
          Pattern pattern = null;
          String realPattern = resolve( getResultFilenamesWildcard() );
          if ( !Utils.isEmpty( realPattern ) ) {
            pattern = Pattern.compile( realPattern );
          }

          for ( Iterator<ResultFile> it = resultFiles.iterator(); it.hasNext() && !parentWorkflow.isStopped(); ) {
            ResultFile resultFile = it.next();
            FileObject file = resultFile.getFile();
            try {
              if ( file != null && file.exists() ) {
                boolean getIt = true;
                if ( pattern != null ) {
                  Matcher matcher = pattern.matcher( file.getName().getBaseName() );
                  getIt = matcher.matches();
                }
                if ( getIt ) {
                  getFileSize( file, result, parentWorkflow );
                }
              }
            } catch ( Exception e ) {
              incrementErrors();
              logError( BaseMessages.getString(
                PKG, "JobEvalFilesMetrics.Error.GettingFileFromResultFilenames", file.toString(), e.toString() ) );
            } finally {
              if ( file != null ) {
                try {
                  file.close();
                } catch ( Exception e ) { /* Ignore */
                }
              }
            }
          }
        }
        break;
      default:
        // static files/folders
        // from grid entered by user
        if ( vSourceFileFolder != null && vSourceFileFolder.length > 0 ) {
          for ( int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++ ) {

            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEvalFilesMetrics.Log.ProcessingRow", vSourceFileFolder[ i ], vwildcard[ i ] ) );
            }

            ProcessFileFolder( vSourceFileFolder[ i ], vwildcard[ i ], vincludeSubFolders[ i ], parentWorkflow, result );
          }
        } else {
          logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Error.FilesGridEmpty" ) );
          return result;
        }
        break;
    }

    result.setResult( isSuccess() );
    result.setNrErrors( getNrError() );
    displayResults();

    return result;
  }

  private void displayResults() {
    if ( isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.Info.FilesCount", String
        .valueOf( getFilesCount() ) ) );
      if ( evaluationType == EVALUATE_TYPE_SIZE ) {
        logDetailed( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.Info.FilesSize", String
          .valueOf( getEvaluationValue() ) ) );
      }
      logDetailed( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.Info.NrErrors", String
        .valueOf( getNrError() ) ) );
      logDetailed( "=======================================" );
    }
  }

  private long getNrError() {
    return this.nrErrors;
  }

  private BigDecimal getEvaluationValue() {
    return this.evaluationValue;
  }

  private BigDecimal getFilesCount() {
    return this.filesCount;
  }

  public int getSuccessConditionType() {
    return successConditionType;
  }

  public void setSuccessConditionType( int successConditionType ) {
    this.successConditionType = successConditionType;
  }

  private boolean isSuccess() {
    boolean retval = false;

    switch ( successConditionType ) {
      case SUCCESS_NUMBER_CONDITION_EQUAL: // equal
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) == 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_DIFFERENT: // different
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) != 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_SMALLER: // smaller
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) < 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_SMALLER_EQUAL: // smaller or equal
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) <= 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_GREATER: // greater
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) > 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_GREATER_EQUAL: // greater or equal
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValue", String
            .valueOf( evaluationValue ), String.valueOf( compareValue ) ) );
        }
        retval = ( getEvaluationValue().compareTo( compareValue ) >= 0 );
        break;
      case SUCCESS_NUMBER_CONDITION_BETWEEN: // between min and max
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.CompareWithValues", String
            .valueOf( evaluationValue ), String.valueOf( minValue ), String.valueOf( maxValue ) ) );
        }
        retval =
          ( getEvaluationValue().compareTo( minValue ) >= 0 && getEvaluationValue().compareTo( maxValue ) <= 0 );
        break;
      default:
        break;
    }

    return retval;
  }

  private void initMetrics() throws Exception {
    evaluationValue = new BigDecimal( 0 );
    filesCount = new BigDecimal( 0 );
    nrErrors = 0;

    if ( successConditionType == SUCCESS_NUMBER_CONDITION_BETWEEN ) {
      minValue = new BigDecimal( resolve( getMinValue() ) );
      maxValue = new BigDecimal( resolve( getMaxValue() ) );
    } else {
      compareValue = new BigDecimal( resolve( getCompareValue() ) );
    }

    if ( evaluationType == EVALUATE_TYPE_SIZE ) {
      int multyply = 1;
      switch ( getScale() ) {
        case SCALE_KBYTES:
          multyply = 1024;
          break;
        case SCALE_MBYTES:
          multyply = 1048576;
          break;
        case SCALE_GBYTES:
          multyply = 1073741824;
          break;
        default:
          break;
      }

      if ( successConditionType == SUCCESS_NUMBER_CONDITION_BETWEEN ) {
        minValue = minValue.multiply( BigDecimal.valueOf( multyply ) );
        maxValue = maxValue.multiply( BigDecimal.valueOf( multyply ) );
      } else {
        compareValue = compareValue.multiply( BigDecimal.valueOf( multyply ) );
      }
    }
    argFromPrevious = ( getSourceFiles() == SOURCE_FILES_PREVIOUS_RESULT );
  }

  private void incrementErrors() {
    nrErrors++;
  }

  public int getSourceFiles() {
    return this.sourceFiles;
  }

  private void incrementFilesCount() {
    filesCount = filesCount.add( ONE );
  }

  public String[] getSourceFileFolder() {
    return sourceFileFolder;
  }

  public void setSourceFileFolder( String[] sourceFileFolder ) {
    this.sourceFileFolder = sourceFileFolder;
  }

  public String[] getSourceWildcard() {
    return sourceWildcard;
  }

  public void setSourceWildcard( String[] sourceWildcard ) {
    this.sourceWildcard = sourceWildcard;
  }

  public String[] getSourceIncludeSubfolders() {
    return sourceIncludeSubfolders;
  }

  public void setSourceIncludeSubfolders( String[] sourceIncludeSubfolders ) {
    this.sourceIncludeSubfolders = sourceIncludeSubfolders;
  }

  public void setSourceFiles( int sourceFiles ) {
    this.sourceFiles = sourceFiles;
  }

  public String getResultFieldFile() {
    return this.ResultFieldFile;
  }

  public void setResultFieldFile( String field ) {
    this.ResultFieldFile = field;
  }

  public String getResultFieldWildcard() {
    return this.ResultFieldWildcard;
  }

  public void setResultFieldWildcard( String field ) {
    this.ResultFieldWildcard = field;
  }

  public String getResultFieldIncludeSubfolders() {
    return this.ResultFieldIncludesubFolders;
  }

  public void setResultFieldIncludeSubfolders( String field ) {
    this.ResultFieldIncludesubFolders = field;
  }

  private void ProcessFileFolder( String sourcefilefoldername, String wildcard, String includeSubfolders,
                                  IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result ) {

    FileObject sourcefilefolder = null;
    FileObject CurrentFile = null;

    // Get real source file and wildcard
    String realSourceFilefoldername = resolve( sourcefilefoldername );
    if ( Utils.isEmpty( realSourceFilefoldername ) ) {
      // Filename is empty!
      logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.log.FileFolderEmpty" ) );
      incrementErrors();
      return;
    }
    String realWildcard = resolve( wildcard );
    final boolean includeSubFolders = YES.equalsIgnoreCase( includeSubfolders );

    try {
      sourcefilefolder = HopVfs.getFileObject( realSourceFilefoldername );

      if ( sourcefilefolder.exists() ) {
        // File exists
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.FileExists", sourcefilefolder
            .toString() ) );
        }

        if ( sourcefilefolder.getType() == FileType.FILE ) {
          // We deals here with a file
          // let's get file size
          getFileSize( sourcefilefolder, result, parentWorkflow );

        } else if ( sourcefilefolder.getType() == FileType.FOLDER ) {
          // We have a folder
          // we will fetch and extract files
          FileObject[] fileObjects = sourcefilefolder.findFiles( new AllFileSelector() {
            public boolean traverseDescendents( FileSelectInfo info ) {
              return info.getDepth() == 0 || includeSubFolders;
            }

            public boolean includeFile( FileSelectInfo info ) {
              FileObject fileObject = info.getFile();
              try {
                if ( fileObject == null ) {
                  return false;
                }
                if ( fileObject.getType() != FileType.FILE ) {
                  return false;
                }
              } catch ( Exception ex ) {
                // Upon error don't process the file.
                return false;
              } finally {
                if ( fileObject != null ) {
                  try {
                    fileObject.close();
                  } catch ( IOException ex ) { /* Ignore */
                  }
                }
              }
              return true;
            }
          } );

          if ( fileObjects != null ) {
            for ( int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++ ) {
              // Fetch files in list one after one ...
              CurrentFile = fileObjects[ j ];

              if ( !CurrentFile.getParent().toString().equals( sourcefilefolder.toString() ) ) {
                // Not in the Base Folder..Only if include sub folders
                if ( includeSubFolders ) {
                  if ( GetFileWildcard( CurrentFile.getName().getBaseName(), realWildcard ) ) {
                    getFileSize( CurrentFile, result, parentWorkflow );
                  }
                }
              } else {
                // In the base folder
                if ( GetFileWildcard( CurrentFile.getName().getBaseName(), realWildcard ) ) {
                  getFileSize( CurrentFile, result, parentWorkflow );
                }
              }
            }
          }
        } else {
          incrementErrors();
          logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Error.UnknowFileFormat", sourcefilefolder
            .toString() ) );
        }
      } else {
        incrementErrors();
        logError( BaseMessages.getString(
          PKG, "JobEvalFilesMetrics.Error.SourceFileNotExists", realSourceFilefoldername ) );
      }
    } catch ( Exception e ) {
      incrementErrors();
      logError( BaseMessages.getString(
        PKG, "JobEvalFilesMetrics.Error.Exception.Processing", realSourceFilefoldername.toString(), e
          .getMessage() ) );

    } finally {
      if ( sourcefilefolder != null ) {
        try {
          sourcefilefolder.close();
        } catch ( IOException ex ) { /* Ignore */
        }

      }
      if ( CurrentFile != null ) {
        try {
          CurrentFile.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }
  }

  private void getFileSize( FileObject file, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow ) {
    try {

      incrementFilesCount();
      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.GetFile", file.toString(), String
          .valueOf( getFilesCount() ) ) );
      }
      switch ( evaluationType ) {
        case EVALUATE_TYPE_SIZE:
          BigDecimal fileSize = BigDecimal.valueOf( file.getContent().getSize() );
          evaluationValue = evaluationValue.add( fileSize );
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Log.AddedFileSize", String
              .valueOf( fileSize ), file.toString() ) );
          }
          break;
        default:
          evaluationValue = evaluationValue.add( ONE );
          break;
      }
    } catch ( Exception e ) {
      incrementErrors();
      logError( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Error.GettingFileSize", file.toString(), e
        .toString() ) );
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean GetFileWildcard( String selectedfile, String wildcard ) {
    Pattern pattern = null;
    boolean getIt = true;

    if ( !Utils.isEmpty( wildcard ) ) {
      pattern = Pattern.compile( wildcard );
      // First see if the file matches the regular expression!
      if ( pattern != null ) {
        Matcher matcher = pattern.matcher( selectedfile );
        getIt = matcher.matches();
      }
    }

    return getIt;
  }

  public void setMinValue( String minvalue ) {
    this.minvalue = minvalue;
  }

  public String getMinValue() {
    return minvalue;
  }

  public void setCompareValue( String comparevalue ) {
    this.comparevalue = comparevalue;
  }

  public String getCompareValue() {
    return comparevalue;
  }

  public void setResultFilenamesWildcard( String resultwildcard ) {
    this.resultFilenamesWildcard = resultwildcard;
  }

  public String getResultFilenamesWildcard() {
    return this.resultFilenamesWildcard;
  }

  public void setMaxValue( String maxvalue ) {
    this.maxvalue = maxvalue;
  }

  public String getMaxValue() {
    return maxvalue;
  }

  public static String getSuccessNumberConditionCode( int i ) {
	    if ( i < 0 || i >= successNumberConditionCode.length ) {
	      return successNumberConditionCode[ 0 ];
	    }
	    return successNumberConditionCode[ i ];
	  }
  
  public static int getSuccessNumberConditionByCode( String tt ) {
	    if ( tt == null ) {
	      return 0;
	    }

	    for ( int i = 0; i < successNumberConditionCode.length; i++ ) {
	      if ( successNumberConditionCode[ i ].equalsIgnoreCase( tt ) ) {
	        return i;
	      }
	    }
	    return 0;
	  }

  public static String getSuccessNumberConditionDesc( int i ) {
	    if ( i < 0 || i >= successNumberConditionDesc.length ) {
	      return successNumberConditionDesc[ 0 ];
	    }
	    return successNumberConditionDesc[ i ];
	  }
  
  public static int getSuccessNumberConditionByDesc( String tt ) {
	    if ( tt == null ) {
	      return 0;
	    }

	    for ( int i = 0; i < successNumberConditionDesc.length; i++ ) {
	      if ( successNumberConditionDesc[ i ].equalsIgnoreCase( tt ) ) {
	        return i;
	      }
	    }

	    // If this fails, try to match using the code.
	    return getSuccessNumberByCode( tt );
	  }
  
  private static int getSuccessNumberByCode( String tt ) {
	    if ( tt == null ) {
	      return 0;
	    }

	    for ( int i = 0; i < successNumberConditionCode.length; i++ ) {
	      if ( successNumberConditionCode[ i ].equalsIgnoreCase( tt ) ) {
	        return i;
	      }
	    }
	    return 0;
	  }
  
  public static int getScaleByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < scaleDesc.length; i++ ) {
      if ( scaleDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getScaleByCode( tt );
  }

  public static int getSourceFilesByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < SourceFilesDesc.length; i++ ) {
      if ( SourceFilesDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getSourceFilesByCode( tt );
  }

  public static int getEvaluationTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < EvaluationTypeDesc.length; i++ ) {
      if ( EvaluationTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getEvaluationTypeByCode( tt );
  }

  private static int getScaleByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < scaleCodes.length; i++ ) {
      if ( scaleCodes[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getSourceFilesByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < SourceFilesCodes.length; i++ ) {
      if ( SourceFilesCodes[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  private static int getEvaluationTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < EvaluationTypeCodes.length; i++ ) {
      if ( EvaluationTypeCodes[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static String getScaleDesc( int i ) {
    if ( i < 0 || i >= scaleDesc.length ) {
      return scaleDesc[ 0 ];
    }
    return scaleDesc[ i ];
  }

  public static String getEvaluationTypeDesc( int i ) {
    if ( i < 0 || i >= EvaluationTypeDesc.length ) {
      return EvaluationTypeDesc[ 0 ];
    }
    return EvaluationTypeDesc[ i ];
  }

  public static String getSourceFilesDesc( int i ) {
    if ( i < 0 || i >= SourceFilesDesc.length ) {
      return SourceFilesDesc[ 0 ];
    }
    return SourceFilesDesc[ i ];
  }

  public static String getScaleCode( int i ) {
    if ( i < 0 || i >= scaleCodes.length ) {
      return scaleCodes[ 0 ];
    }
    return scaleCodes[ i ];
  }

  public static String getSourceFilesCode( int i ) {
    if ( i < 0 || i >= SourceFilesCodes.length ) {
      return SourceFilesCodes[ 0 ];
    }
    return SourceFilesCodes[ i ];
  }

  public static String getEvaluationTypeCode( int i ) {
    if ( i < 0 || i >= EvaluationTypeCodes.length ) {
      return EvaluationTypeCodes[ 0 ];
    }
    return EvaluationTypeCodes[ i ];
  }

  public int getScale() {
    return this.scale;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    boolean res = ActionValidatorUtils.andValidator().validate( this, "arguments", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );

    if ( res == false ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notNullValidator(),
      ActionValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < sourceFileFolder.length; i++ ) {
      ActionValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

  @Override public boolean isEvaluation() {
    return true;
  }

}
