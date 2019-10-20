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

package org.apache.hop.job.entries.filesexist;

import java.io.IOException;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines a Files exist job entry.
 *
 * @author Samatar
 * @since 10-12-2007
 *
 */

public class JobEntryFilesExist extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryFilesExist.class; // for i18n purposes, needed by Translator2!!

  private String filename; // TODO: looks like it is not used: consider deleting

  private String[] arguments;

  public JobEntryFilesExist( String n ) {
    super( n, "" );
    filename = null;
  }

  public JobEntryFilesExist() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    arguments = new String[nrFields];
  }

  public Object clone() {
    JobEntryFilesExist je = (JobEntryFilesExist) super.clone();
    if ( arguments != null ) {
      int nrFields = arguments.length;
      je.allocate( nrFields );
      System.arraycopy( arguments, 0, je.arguments, 0, nrFields );
    }
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 30 );

    retval.append( super.getXML() );
    if ( parentJobMeta != null ) {
      parentJobMeta.getNamedClusterEmbedManager().registerUrl( filename );
    }
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XMLHandler.addTagValue( "name", arguments[i] ) );
        retval.append( "        </field>" ).append( Const.CR );
        if ( parentJobMeta != null ) {
          parentJobMeta.getNamedClusterEmbedManager().registerUrl( arguments[i] );
        }
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
    Repository rep, IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );
      filename = XMLHandler.getTagValue( entrynode, "filename" );

      Node fields = XMLHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XMLHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        arguments[i] = XMLHandler.getTagValue( fnode, "name" );

      }
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryFilesExist.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node", xe.getMessage() ) );
    }
  }

  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
    List<SlaveServer> slaveServers ) throws HopException {
    try {
      filename = rep.getJobEntryAttributeString( id_jobentry, "filename" );

      // How many arguments?
      int argnr = rep.countNrJobEntryAttributes( id_jobentry, "name" );
      allocate( argnr );

      // Read them all...
      for ( int a = 0; a < argnr; a++ ) {
        arguments[a] = rep.getJobEntryAttributeString( id_jobentry, a, "name" );
      }
    } catch ( HopException dbe ) {
      throw new HopException( BaseMessages
        .getString( PKG, "JobEntryFilesExist.ERROR_0002_Cannot_Load_Job_From_Repository", "" + id_jobentry, dbe
          .getMessage() ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws HopException {
    try {
      rep.saveJobEntryAttribute( id_job, getObjectId(), "filename", filename );

      // save the arguments...
      if ( arguments != null ) {
        for ( int i = 0; i < arguments.length; i++ ) {
          rep.saveJobEntryAttribute( id_job, getObjectId(), i, "name", arguments[i] );
        }
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "JobEntryFilesExist.ERROR_0003_Cannot_Save_Job_Entry", "" + id_job, dbe.getMessage() ) );
    }
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setArguments( String[] arguments ) {
    this.arguments = arguments;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 0 );
    int missingfiles = 0;
    int nrErrors = 0;

    // see PDI-10270 for details
    boolean oldBehavior =
      "Y".equalsIgnoreCase( getVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_JOB_ENTRIES, "N" ) );

    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length && !parentJob.isStopped(); i++ ) {
        FileObject file = null;

        try {
          String realFilefoldername = environmentSubstitute( arguments[i] );
          //Set Embedded NamedCluter MetatStore Provider Key so that it can be passed to VFS
          if ( parentJobMeta.getNamedClusterEmbedManager() != null ) {
            parentJobMeta.getNamedClusterEmbedManager()
              .passEmbeddedMetastoreKey( this, parentJobMeta.getEmbeddedMetastoreProviderKey() );
          }
          file = HopVFS.getFileObject( realFilefoldername, this );

          if ( file.exists() && file.isReadable() ) { // TODO: is it needed to check file for readability?
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobEntryFilesExist.File_Exists", realFilefoldername ) );
            }
          } else {
            missingfiles++;
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobEntryFilesExist.File_Does_Not_Exist", realFilefoldername ) );
            }
          }

        } catch ( Exception e ) {
          nrErrors++;
          missingfiles++;
          logError( BaseMessages.getString( PKG, "JobEntryFilesExist.ERROR_0004_IO_Exception", e.toString() ), e );
        } finally {
          if ( file != null ) {
            try {
              file.close();
              file = null;
            } catch ( IOException ex ) { /* Ignore */
            }
          }
        }
      }

    }

    result.setNrErrors( nrErrors );

    if ( oldBehavior ) {
      result.setNrErrors( missingfiles );
    }

    if ( missingfiles == 0 ) {
      result.setResult( true );
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
  }

}
