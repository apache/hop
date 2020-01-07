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

package org.apache.hop.job.entries.fileexists;

import java.util.List;
import java.util.Map;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
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
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;

import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines an SQL job entry.
 *
 * @author Matt
 * @since 05-11-2003
 *
 */

public class JobEntryFileExists extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryFileExists.class; // for i18n purposes, needed by Translator2!!

  private String filename;

  public JobEntryFileExists( String n ) {
    super( n, "" );
    filename = null;
  }

  public JobEntryFileExists() {
    this( "" );
  }

  public Object clone() {
    JobEntryFileExists je = (JobEntryFileExists) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 20 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode, List<SlaveServer> slaveServers,
    IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode, slaveServers );
      filename = XMLHandler.getTagValue( entrynode, "filename" );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryFileExists.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node" ), xe );
    }
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getRealFilename() {
    return environmentSubstitute( getFilename() );
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 0 );

    if ( filename != null ) {

      String realFilename = getRealFilename();
      try {
        FileObject file = HopVFS.getFileObject( realFilename, this );
        if ( file.exists() && file.isReadable() ) {
          logDetailed( BaseMessages.getString( PKG, "JobEntryFileExists.File_Exists", realFilename ) );
          result.setResult( true );
        } else {
          logDetailed( BaseMessages.getString( PKG, "JobEntryFileExists.File_Does_Not_Exist", realFilename ) );
        }
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobEntryFileExists.ERROR_0004_IO_Exception", e.getMessage() ), e );
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntryFileExists.ERROR_0005_No_Filename_Defined" ) );
    }

    return result;
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( filename ) ) {
      String realFileName = jobMeta.environmentSubstitute( filename );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param space
   *          The variable space to resolve (environment) variables with.
   * @param definitions
   *          The map containing the filenames and content
   * @param namingInterface
   *          The resource naming interface allows the object to be named appropriately
   * @param repository
   *          The repository to load resources from
   * @param metaStore
   *          the metaStore to load external metadata from
   *
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException
   *           in case something goes wrong during the export
   */
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
    ResourceNamingInterface namingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous steps, forget about this!
      //
      if ( !Utils.isEmpty( filename ) ) {
        // From : ${FOLDER}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVFS.getFileObject( space.environmentSubstitute( filename ), space );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          filename = namingInterface.nameResource( fileObject, space, true );

          return filename;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }
}
