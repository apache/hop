/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.job.entries.copymoveresultfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileUtil;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.job.entry.validator.ValidatorContext;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'copymoveresultfilenames' job entry. Its main use would be to copy or move files in the result
 * filenames to a destination folder. that can be used to control the flow in ETL cycles.
 *
 * @author Samatar
 * @since 25-02-2008
 */

@JobEntry( 
		id = "COPY_MOVE_RESULT_FILENAMES",
		i18nPackageName = "org.apache.hop.job.entries.copymoveresultfilenames",
		name = "JobEntryCopyMoveResultFilenames.Name",
		description = "JobEntryCopyMoveResultFilenames.Description",
		image = "CopyMoveResultFilenames.svg",
		categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement" 
)
public class JobEntryCopyMoveResultFilenames extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryCopyMoveResultFilenames.class; // for i18n purposes, needed by Translator2!!

  private String foldername;
  private boolean specifywildcard;
  private String wildcard;
  private String wildcardexclude;
  private String destination_folder;
  private String nr_errors_less_than;

  public String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";
  private String success_condition;
  private Pattern wildcardPattern;
  private Pattern wildcardExcludePattern;

  private boolean add_date;
  private boolean add_time;
  private boolean SpecifyFormat;
  private String date_time_format;
  private boolean AddDateBeforeExtension;
  private String action;

  private boolean OverwriteFile;
  private boolean CreateDestinationFolder;
  boolean RemovedSourceFilename;
  boolean AddDestinationFilename;

  int NrErrors = 0;
  private int NrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public JobEntryCopyMoveResultFilenames( String n ) {
    super( n, "" );
    RemovedSourceFilename = true;
    AddDestinationFilename = true;
    CreateDestinationFolder = false;
    foldername = null;
    wildcardexclude = null;
    wildcard = null;
    specifywildcard = false;

    OverwriteFile = false;
    add_date = false;
    add_time = false;
    SpecifyFormat = false;
    date_time_format = null;
    AddDateBeforeExtension = false;
    destination_folder = null;
    nr_errors_less_than = "10";

    action = "copy";
    success_condition = SUCCESS_IF_NO_ERRORS;
  }

  public JobEntryCopyMoveResultFilenames() {
    this( "" );
  }

  public Object clone() {
    JobEntryCopyMoveResultFilenames je = (JobEntryCopyMoveResultFilenames) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 ); // 358 chars in just tags and spaces alone

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "foldername", foldername ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "specify_wildcard", specifywildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wildcardexclude", wildcardexclude ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "destination_folder", destination_folder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "nr_errors_less_than", nr_errors_less_than ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "success_condition", success_condition ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_date", add_date ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_time", add_time ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "SpecifyFormat", SpecifyFormat ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "date_time_format", date_time_format ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "action", action ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "AddDateBeforeExtension", AddDateBeforeExtension ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "OverwriteFile", OverwriteFile ) );
    retval
      .append( "      " ).append( XMLHandler.addTagValue( "CreateDestinationFolder", CreateDestinationFolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "RemovedSourceFilename", RemovedSourceFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "AddDestinationFilename", AddDestinationFilename ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      foldername = XMLHandler.getTagValue( entrynode, "foldername" );
      specifywildcard = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "specify_wildcard" ) );
      wildcard = XMLHandler.getTagValue( entrynode, "wildcard" );
      wildcardexclude = XMLHandler.getTagValue( entrynode, "wildcardexclude" );
      destination_folder = XMLHandler.getTagValue( entrynode, "destination_folder" );
      nr_errors_less_than = XMLHandler.getTagValue( entrynode, "nr_errors_less_than" );
      success_condition = XMLHandler.getTagValue( entrynode, "success_condition" );
      add_date = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_date" ) );
      add_time = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_time" ) );
      SpecifyFormat = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      AddDateBeforeExtension =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "AddDateBeforeExtension" ) );

      date_time_format = XMLHandler.getTagValue( entrynode, "date_time_format" );
      action = XMLHandler.getTagValue( entrynode, "action" );

      OverwriteFile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "OverwriteFile" ) );
      CreateDestinationFolder =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "CreateDestinationFolder" ) );
      RemovedSourceFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "RemovedSourceFilename" ) );
      AddDestinationFilename =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "AddDestinationFilename" ) );

    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryCopyMoveResultFilenames.CanNotLoadFromXML", xe.getMessage() ) );
    }
  }

  public void setSpecifyWildcard( boolean specifywildcard ) {
    this.specifywildcard = specifywildcard;
  }

  public boolean isSpecifyWildcard() {
    return specifywildcard;
  }

  public void setFoldername( String foldername ) {
    this.foldername = foldername;
  }

  public String getFoldername() {
    return foldername;
  }

  public String getWildcard() {
    return wildcard;
  }

  public String getWildcardExclude() {
    return wildcardexclude;
  }

  public String getRealWildcard() {
    return environmentSubstitute( getWildcard() );
  }

  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  public void setWildcardExclude( String wildcardexclude ) {
    this.wildcardexclude = wildcardexclude;
  }

  public void setAddDate( boolean adddate ) {
    this.add_date = adddate;
  }

  public boolean isAddDate() {
    return add_date;
  }

  public void setAddTime( boolean addtime ) {
    this.add_time = addtime;
  }

  public boolean isAddTime() {
    return add_time;
  }

  public void setAddDateBeforeExtension( boolean AddDateBeforeExtension ) {
    this.AddDateBeforeExtension = AddDateBeforeExtension;
  }

  public boolean isAddDateBeforeExtension() {
    return AddDateBeforeExtension;
  }

  public boolean isOverwriteFile() {
    return OverwriteFile;
  }

  public void setOverwriteFile( boolean OverwriteFile ) {
    this.OverwriteFile = OverwriteFile;
  }

  public void setCreateDestinationFolder( boolean CreateDestinationFolder ) {
    this.CreateDestinationFolder = CreateDestinationFolder;
  }

  public boolean isCreateDestinationFolder() {
    return CreateDestinationFolder;
  }

  public boolean isRemovedSourceFilename() {
    return RemovedSourceFilename;
  }

  public void setRemovedSourceFilename( boolean RemovedSourceFilename ) {
    this.RemovedSourceFilename = RemovedSourceFilename;
  }

  public void setAddDestinationFilename( boolean AddDestinationFilename ) {
    this.AddDestinationFilename = AddDestinationFilename;
  }

  public boolean isAddDestinationFilename() {
    return AddDestinationFilename;
  }

  public boolean isSpecifyFormat() {
    return SpecifyFormat;
  }

  public void setSpecifyFormat( boolean SpecifyFormat ) {
    this.SpecifyFormat = SpecifyFormat;
  }

  public void setDestinationFolder( String destinationFolder ) {
    this.destination_folder = destinationFolder;
  }

  public String getDestinationFolder() {
    return destination_folder;
  }

  public void setNrErrorsLessThan( String nr_errors_less_than ) {
    this.nr_errors_less_than = nr_errors_less_than;
  }

  public String getNrErrorsLessThan() {
    return nr_errors_less_than;
  }

  public void setSuccessCondition( String success_condition ) {
    this.success_condition = success_condition;
  }

  public String getSuccessCondition() {
    return success_condition;
  }

  public void setAction( String action ) {
    this.action = action;
  }

  public String getAction() {
    return action;
  }

  public String getDateTimeFormat() {
    return date_time_format;
  }

  public void setDateTimeFormat( String date_time_format ) {
    this.date_time_format = date_time_format;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setNrErrors( 1 );
    result.setResult( false );

    boolean deleteFile = getAction().equals( "delete" );

    String realdestinationFolder = null;
    if ( !deleteFile ) {
      realdestinationFolder = environmentSubstitute( getDestinationFolder() );

      if ( !CreateDestinationFolder( realdestinationFolder ) ) {
        return result;
      }
    }
    if ( !Utils.isEmpty( wildcard ) ) {
      wildcardPattern = Pattern.compile( environmentSubstitute( wildcard ) );
    }
    if ( !Utils.isEmpty( wildcardexclude ) ) {
      wildcardExcludePattern = Pattern.compile( environmentSubstitute( wildcardexclude ) );
    }

    if ( previousResult != null ) {
      NrErrors = 0;
      limitFiles = Const.toInt( environmentSubstitute( getNrErrorsLessThan() ), 10 );
      NrErrors = 0;
      NrSuccess = 0;
      successConditionBroken = false;
      successConditionBrokenExit = false;

      FileObject file = null;

      try {
        int size = result.getResultFiles().size();
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.log.FilesFound", "" + size ) );
        }

        List<ResultFile> resultFiles = result.getResultFilesList();
        if ( resultFiles != null && resultFiles.size() > 0 ) {
          for ( Iterator<ResultFile> it = resultFiles.iterator(); it.hasNext() && !parentJob.isStopped(); ) {
            if ( successConditionBroken ) {
              logError( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.Error.SuccessConditionbroken", "" + NrErrors ) );
              throw new Exception( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.Error.SuccessConditionbroken", "" + NrErrors ) );
            }

            ResultFile resultFile = it.next();
            file = resultFile.getFile();
            if ( file != null && file.exists() ) {
              if ( !specifywildcard
                || ( CheckFileWildcard( file.getName().getBaseName(), wildcardPattern, true )
                && !CheckFileWildcard( file.getName().getBaseName(), wildcardExcludePattern, false )
                && specifywildcard ) ) {
                // Copy or Move file
                if ( !processFile( file, realdestinationFolder, result, parentJob, deleteFile ) ) {
                  // Update Errors
                  updateErrors();
                }
              }

            } else {
              logError( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.log.ErrorCanNotFindFile", file.toString() ) );
              // Update Errors
              updateErrors();
            }
          } // end for
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.Error", e.toString() ) );
      } finally {
        if ( file != null ) {
          try {
            file.close();
            file = null;
          } catch ( Exception ex ) { /* Ignore */
          }
        }
      }
    }
    // Success Condition
    result.setNrErrors( NrErrors );
    result.setNrLinesWritten( NrSuccess );
    if ( getSuccessStatus() ) {
      result.setResult( true );
    }

    return result;
  }

  private void updateErrors() {
    NrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( NrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrErrors >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ( ( NrErrors == 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrSuccess >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) )
      || ( NrErrors <= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }

    return retval;
  }

  private boolean CreateDestinationFolder( String foldername ) {
    FileObject folder = null;
    try {
      folder = HopVFS.getFileObject( foldername, this );

      if ( !folder.exists() ) {
        logError( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.Log.FolderNotExists", foldername ) );
        if ( isCreateDestinationFolder() ) {
          folder.createFolder();
        } else {
          return false;
        }
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.Log.FolderCreated", foldername ) );
        }
      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages
            .getString( PKG, "JobEntryCopyMoveResultFilenames.Log.FolderExists", foldername ) );
        }
      }
      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString(
        PKG, "JobEntryCopyMoveResultFilenames.Log.CanNotCreatedFolder", foldername, e.toString() ) );

    } finally {
      if ( folder != null ) {
        try {
          folder.close();
          folder = null;
        } catch ( Exception ex ) { /* Ignore */
        }
      }
    }
    return false;
  }

  private boolean processFile( FileObject sourcefile, String destinationFolder, Result result, Job parentJob,
                               boolean deleteFile ) {
    boolean retval = false;

    try {
      if ( deleteFile ) {
        // delete file
        if ( sourcefile.delete() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobEntryCopyMoveResultFilenames.log.DeletedFile", sourcefile.toString() ) );
          }

          // Remove source file from result files list
          result.getResultFiles().remove( sourcefile.toString() );
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobEntryCopyMoveResultFilenames.RemovedFileFromResult", sourcefile.toString() ) );
          }

        } else {
          logError( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.CanNotDeletedFile", sourcefile
            .toString() ) );
        }
      } else {
        // return destination short filename
        String shortfilename = getDestinationFilename( sourcefile.getName().getBaseName() );
        // build full destination filename
        String destinationFilename = destinationFolder + Const.FILE_SEPARATOR + shortfilename;
        FileObject destinationfile = HopVFS.getFileObject( destinationFilename, this );
        boolean filexists = destinationfile.exists();
        if ( filexists ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobEntryCopyMoveResultFilenames.Log.FileExists", destinationFilename ) );
          }
        }
        if ( ( !filexists ) || ( filexists && isOverwriteFile() ) ) {
          if ( getAction().equals( "copy" ) ) {
            // Copy file
            FileUtil.copyContent( sourcefile, destinationfile );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.log.CopiedFile", sourcefile.toString(), destinationFolder ) );
            }
          } else {
            // Move file
            sourcefile.moveTo( destinationfile );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.log.MovedFile", sourcefile.toString(), destinationFolder ) );
            }
          }
          if ( isRemovedSourceFilename() ) {
            // Remove source file from result files list
            result.getResultFiles().remove( sourcefile.toString() );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.RemovedFileFromResult", sourcefile.toString() ) );
            }
          }
          if ( isAddDestinationFilename() ) {
            // Add destination filename to Resultfilenames ...
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVFS.getFileObject(
                destinationfile.toString(), this ), parentJob.getJobname(), toString() );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryCopyMoveResultFilenames.AddedFileToResult", destinationfile.toString() ) );
            }
          }
        }
      }
      retval = true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobEntryCopyMoveResultFilenames.Log.ErrorProcessing", e.toString() ) );
    }

    return retval;
  }

  private String getDestinationFilename( String shortsourcefilename ) throws Exception {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf( '.' );
    if ( isAddDateBeforeExtension() ) {
      shortfilename = shortfilename.substring( 0, lastindexOfDot );
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if ( isSpecifyFormat() && !Utils.isEmpty( getDateTimeFormat() ) ) {
      daf.applyPattern( getDateTimeFormat() );
      String dt = daf.format( now );
      shortfilename += dt;
    } else {
      if ( isAddDate() ) {
        daf.applyPattern( "yyyyMMdd" );
        String d = daf.format( now );
        shortfilename += "_" + d;
      }
      if ( isAddTime() ) {
        daf.applyPattern( "HHmmssSSS" );
        String t = daf.format( now );
        shortfilename += "_" + t;
      }
    }
    if ( isAddDateBeforeExtension() ) {
      shortfilename += shortsourcefilename.substring( lastindexOfDot, lenstring );
    }

    return shortfilename;
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param pattern
   * @param include
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean CheckFileWildcard( String selectedfile, Pattern pattern, boolean include ) {
    boolean getIt = include;
    if ( pattern != null ) {
      Matcher matcher = pattern.matcher( selectedfile );
      getIt = matcher.matches();
    }
    return getIt;
  }

  public boolean evaluates() {
    return true;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(), JobEntryValidatorUtils.fileDoesNotExistValidator() );
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks, ctx );
  }

}
