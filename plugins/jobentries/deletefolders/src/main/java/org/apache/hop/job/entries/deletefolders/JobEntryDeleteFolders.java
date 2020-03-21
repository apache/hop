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

package org.apache.hop.job.entries.deletefolders;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.job.entry.validator.ValidatorContext;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'delete folders' job entry.
 *
 * @author Samatar Hassan
 * @since 13-05-2008
 */

@JobEntry(
  id = "DELETE_FOLDERS",
  i18nPackageName = "org.apache.hop.job.entries.deletefolders",
  name = "JobEntryDeleteFolders.Name",
  description = "JobEntryDeleteFolders.Description",
  image = "DeleteFolders.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement"
)
public class JobEntryDeleteFolders extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static final Class<?> PKG = JobEntryDeleteFolders.class; // for i18n purposes, needed by Translator2!!

  public boolean argFromPrevious;

  public String[] arguments;

  private String successCondition;
  public static final String SUCCESS_IF_AT_LEAST_X_FOLDERS_DELETED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private String limitFolders;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int nrLimitFolders = 0;

  public JobEntryDeleteFolders( String name ) {
    super( name, "" );
    argFromPrevious = false;
    arguments = null;

    successCondition = SUCCESS_IF_NO_ERRORS;
    limitFolders = "10";
  }

  public JobEntryDeleteFolders() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    arguments = new String[ nrFields ];
  }

  public Object clone() {
    JobEntryDeleteFolders je = (JobEntryDeleteFolders) super.clone();
    if ( arguments != null ) {
      int nrFields = arguments.length;
      je.allocate( nrFields );
      System.arraycopy( arguments, 0, je.arguments, 0, nrFields );
    }
    return je;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "arg_from_previous", argFromPrevious ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "success_condition", successCondition ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "limit_folders", limitFolders ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XMLHandler.addTagValue( "name", arguments[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      argFromPrevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "arg_from_previous" ) );
      successCondition = XMLHandler.getTagValue( entrynode, "success_condition" );
      limitFolders = XMLHandler.getTagValue( entrynode, "limit_folders" );

      Node fields = XMLHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XMLHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        arguments[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "JobEntryDeleteFolders.UnableToLoadFromXml" ), xe );
    }
  }

  @Override
  public Result execute( Result result, int nr ) throws HopException {
    List<RowMetaAndData> rows = result.getRows();

    result.setNrErrors( 1 );
    result.setResult( false );

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    nrLimitFolders = Const.toInt( environmentSubstitute( getLimitFolders() ), 10 );

    if ( argFromPrevious && log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryDeleteFolders.FoundPreviousRows", String
          .valueOf( ( rows != null ? rows.size() : 0 ) ) ) );      
    }

    if ( argFromPrevious && rows != null ) {
      for ( int iteration = 0; iteration < rows.size() && !parentJob.isStopped(); iteration++ ) {
        if ( successConditionBroken ) {
          logError( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Error.SuccessConditionbroken", ""
            + nrErrors ) );
          result.setNrErrors( nrErrors );
          result.setNrLinesDeleted( nrSuccess );
          return result;
        }
        RowMetaAndData resultRow = rows.get( iteration );
        String argsPrevious = resultRow.getString( 0, null );
        if ( !Utils.isEmpty( argsPrevious ) ) {
          if ( deleteFolder( argsPrevious ) ) {
            updateSuccess();
          } else {
            updateErrors();
          }
        } else {
          // empty filename !
          logError( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Error.EmptyLine" ) );
        }
      }
    } else if ( arguments != null ) {
      for ( int i = 0; i < arguments.length && !parentJob.isStopped(); i++ ) {
        if ( successConditionBroken ) {
          logError( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Error.SuccessConditionbroken", ""
            + nrErrors ) );
          result.setNrErrors( nrErrors );
          result.setNrLinesDeleted( nrSuccess );
          return result;
        }
        String realfilename = environmentSubstitute( arguments[ i ] );
        if ( !Utils.isEmpty( realfilename ) ) {
          if ( deleteFolder( realfilename ) ) {
            updateSuccess();
          } else {
            updateErrors();
          }
        } else {
          // empty filename !
          logError( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Error.EmptyLine" ) );
        }
      }
    }

    if ( log.isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Log.Info.NrError", "" + nrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Log.Info.NrDeletedFolders", "" + nrSuccess ) );
      logDetailed( "=======================================" );
    }

    result.setNrErrors( nrErrors );
    result.setNrLinesDeleted( nrSuccess );
    if ( getSuccessStatus() ) {
      result.setResult( true );
    }

    return result;
  }

  private void updateErrors() {
    nrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( nrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( nrErrors >= nrLimitFolders && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ( ( nrErrors == 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( nrSuccess >= nrLimitFolders && getSuccessCondition().equals( SUCCESS_IF_AT_LEAST_X_FOLDERS_DELETED ) )
      || ( nrErrors <= nrLimitFolders && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }

    return retval;
  }

  private boolean deleteFolder( String foldername ) {
    boolean rcode = false;
    FileObject filefolder = null;

    try {
      filefolder = HopVFS.getFileObject( foldername, this );

      if ( filefolder.exists() ) {
        // the file or folder exists
        if ( filefolder.getType() == FileType.FOLDER ) {
          // It's a folder
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobEntryDeleteFolders.ProcessingFolder", foldername ) );
          }
          // Delete Files
          int count = filefolder.delete( new TextFileSelector() );

          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobEntryDeleteFolders.TotalDeleted", foldername, String
              .valueOf( count ) ) );
          }
          rcode = true;
        } else {
          // Error...This file is not a folder!
          logError( BaseMessages.getString( PKG, "JobEntryDeleteFolders.Error.NotFolder" ) );
        }
      } else {
        // File already deleted, no reason to try to delete it
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "JobEntryDeleteFolders.FolderAlreadyDeleted", foldername ) );
        }
        rcode = true;
      }
    } catch ( Exception e ) {
      logError(
        BaseMessages.getString( PKG, "JobEntryDeleteFolders.CouldNotDelete", foldername, e.getMessage() ), e );
    } finally {
      if ( filefolder != null ) {
        try {
          filefolder.close();
        } catch ( IOException ex ) {
          // Ignore
        }
      }
    }

    return rcode;
  }

  private class TextFileSelector implements FileSelector {
    public boolean includeFile( FileSelectInfo info ) {
      return true;
    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return true;
    }
  }

  public void setPrevious( boolean argFromPrevious ) {
    this.argFromPrevious = argFromPrevious;
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    boolean res = JobEntryValidatorUtils.andValidator().validate( this, "arguments", remarks, AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );

    if ( !res ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(), JobEntryValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < arguments.length; i++ ) {
      JobEntryValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( arguments != null ) {
      ResourceReference reference = null;
      for ( int i = 0; i < arguments.length; i++ ) {
        String filename = jobMeta.environmentSubstitute( arguments[ i ] );
        if ( reference == null ) {
          reference = new ResourceReference( this );
          references.add( reference );
        }
        reference.getEntries().add( new ResourceEntry( filename, ResourceType.FILE ) );
      }
    }
    return references;
  }

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setSuccessCondition( String successCondition ) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void setLimitFolders( String limitFolders ) {
    this.limitFolders = limitFolders;
  }

  public String getLimitFolders() {
    return limitFolders;
  }

}
