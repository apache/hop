/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.talendjobexec;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.plugins.HopURLClassLoader;
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
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This executes an exported Talend Job.
 *
 * @author Matt
 * @since 1-04-2011
 */

@JobEntry(
  id = "TALEND_JOB_EXEC",
  i18nPackageName = "org.apache.hop.job.entries.talendjobexec",
  name = "JobEntryTalendJobExec.Name",
  description = "JobEntryTalendJobExec.Description",
  image = "TalendJobExec.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Deprecated"
)
public class JobEntryTalendJobExec extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryTalendJobExec.class; // for i18n

  private static Map<String, ClassLoader> classLoaderCache = new ConcurrentHashMap<>();

  private String filename;
  private String className;

  public JobEntryTalendJobExec( String n ) {
    super( n, "" );
    filename = null;
  }

  public JobEntryTalendJobExec() {
    this( "" );
  }

  public Object clone() {
    JobEntryTalendJobExec je = (JobEntryTalendJobExec) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "class_name", className ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      filename = XMLHandler.getTagValue( entrynode, "filename" );
      className = XMLHandler.getTagValue( entrynode, "class_name" );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryTalendJobExec.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node" ), xe );
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

    if ( filename != null ) {
      String realFilename = getRealFilename();
      try {
        FileObject file = HopVFS.getFileObject( realFilename, this );
        if ( file.exists() && file.isReadable() ) {
          result = executeTalenJob( file, result, nr );
        } else {
          logDetailed( BaseMessages.getString( PKG, "JobEntryTalendJobExec.File_Does_Not_Exist", realFilename ) );
        }
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError(
          BaseMessages.getString( PKG, "JobEntryTalendJobExec.ERROR_0004_IO_Exception", e.getMessage() ), e );
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobEntryTalendJobExec.ERROR_0005_No_Filename_Defined" ) );
    }

    return result;
  }

  private Result executeTalenJob( FileObject file, Result result, int nr ) throws Exception {

    ClassLoader classLoader = null;
    try {

      classLoader = classLoaderCache.get( file.toString() );
      if ( classLoader == null ) {
        // Find the jar files in the ZIP file...
        //
        final URL[] jarFiles = prepareJarFiles( file );

        // Create a new class loader with the extracted jar files.
        //
        classLoader = new HopURLClassLoader( jarFiles, getClass().getClassLoader() );

        Runtime.getRuntime().addShutdownHook( new Thread() {

          @Override
          public void run() {
            try {
              cleanupJarFiles( jarFiles );
            } catch ( Exception e ) {
              System.err.println( "Error cleaning up temporary Talend jar file extracts: "
                + Const.getStackTracker( e ) );
            }
          }
        } );

        classLoaderCache.put( file.toString(), classLoader );
      }

      Class<?> clazz = classLoader.loadClass( environmentSubstitute( getClassName() ) );
      Object jobObject = clazz.newInstance();
      Method runJob = clazz.getMethod( "runJobInTOS", String[].class );

      // TODO: consider passing something of some sort in this next method:
      // variables, arguments...
      //
      int returnCode = (Integer) runJob.invoke( jobObject, (Object) new String[] {} );

      result.setExitStatus( returnCode );
      result.setResult( true );
      result.setNrErrors( 0 );

    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "JobEntryTalendJobExec.ERROR_0006_ExceptionExecutingTalenJob" ), e );
    }

    return result;
  }

  private URL[] prepareJarFiles( FileObject zipFile ) throws Exception {

    // zip:file:///tmp/foo.zip
    FileInputList fileList = FileInputList.createFileList( this, new String[] { "zip:" + zipFile.toString(), },
      new String[] { ".*\\.jar$", }, // Include mask: only jar files
      new String[] { ".*classpath\\.jar$", }, // Exclude mask: only jar files
      new String[] { "Y", }, // File required
      new boolean[] { true, } ); // Search sub-directories

    List<URL> files = new ArrayList<URL>();

    // Copy the jar files in the temp folder...
    //
    for ( FileObject file : fileList.getFiles() ) {
      FileObject jarfilecopy =
        HopVFS.createTempFile(
          file.getName().getBaseName(), ".jar", environmentSubstitute( "${java.io.tmpdir}" ) );
      jarfilecopy.copyFrom( file, new AllFileSelector() );
      files.add( jarfilecopy.getURL() );
    }

    return files.toArray( new URL[ files.size() ] );
  }

  private void cleanupJarFiles( URL[] jarFiles ) throws Exception {
    if ( jarFiles == null ) {
      return;
    }

    for ( URL jarFile : jarFiles ) {
      File file = new File( jarFile.toURI() );
      file.delete();
    }
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
   * Since the exported job that runs this will reside in a ZIP file, we can't reference files relatively. So what this
   * does is turn the name of files into absolute paths OR it simply includes the resource in the ZIP file. For now,
   * we'll simply turn it into an absolute path and pray that the file is on a shared drive or something like that.
   * TODO: create options to configure this behavior
   * <p>
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param space           The variable space to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metaStore       the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface namingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the
      // file object...
      // In case the name of the file comes from previous steps, forget about
      // this!
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

  /**
   * @return the className
   */
  public String getClassName() {
    return className;
  }

  /**
   * @param className the className to set
   */
  public void setClassName( String className ) {
    this.className = className;
  }
}
