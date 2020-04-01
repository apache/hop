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

package org.apache.hop.pipeline.steps.xbaseinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/*
 * Created on 2-jun-2003
 *
 */

public class XBaseInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = XBaseInputMeta.class; // for i18n purposes, needed by Translator!!

  private String dbfFileName;
  private int rowLimit;
  private boolean rowNrAdded;
  private String rowNrField;

  /**
   * Are we accepting filenames in input rows?
   */
  private boolean acceptingFilenames;

  /**
   * The field in which the filename is placed
   */
  private String acceptingField;

  /**
   * The stepname to accept filenames from
   */
  private String acceptingStepName;

  /**
   * The step to accept filenames from
   */
  private StepMeta acceptingStep;

  /**
   * Flag indicating that we should include the filename in the output
   */
  private boolean includeFilename;

  /**
   * The name of the field in the output containing the filename
   */
  private String filenameField;

  /**
   * The character set / encoding used in the string or memo fields
   */
  private String charactersetName;

  public XBaseInputMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the dbfFileName.
   */
  public String getDbfFileName() {
    return dbfFileName;
  }

  /**
   * @param dbfFileName The dbfFileName to set.
   */
  public void setDbfFileName( String dbfFileName ) {
    this.dbfFileName = dbfFileName;
  }

  /**
   * @return Returns the rowLimit.
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( int rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the rowNrField.
   */
  public String getRowNrField() {
    return rowNrField;
  }

  /**
   * @param rowNrField The rowNrField to set.
   */
  public void setRowNrField( String rowNrField ) {
    this.rowNrField = rowNrField;
  }

  /**
   * @return Returns the rowNrAdded.
   */
  public boolean isRowNrAdded() {
    return rowNrAdded;
  }

  /**
   * @param rowNrAdded The rowNrAdded to set.
   */
  public void setRowNrAdded( boolean rowNrAdded ) {
    this.rowNrAdded = rowNrAdded;
  }

  /**
   * @return Returns the acceptingField.
   */
  public String getAcceptingField() {
    return acceptingField;
  }

  /**
   * @param acceptingField The acceptingField to set.
   */
  public void setAcceptingField( String acceptingField ) {
    this.acceptingField = acceptingField;
  }

  /**
   * @return Returns the acceptingFilenames.
   */
  public boolean isAcceptingFilenames() {
    return acceptingFilenames;
  }

  /**
   * @param acceptingFilenames The acceptingFilenames to set.
   */
  public void setAcceptingFilenames( boolean acceptingFilenames ) {
    this.acceptingFilenames = acceptingFilenames;
  }

  /**
   * @return Returns the acceptingStep.
   */
  public StepMeta getAcceptingStep() {
    return acceptingStep;
  }

  /**
   * @param acceptingStep The acceptingStep to set.
   */
  public void setAcceptingStep( StepMeta acceptingStep ) {
    this.acceptingStep = acceptingStep;
  }

  /**
   * @return Returns the acceptingStepName.
   */
  public String getAcceptingStepName() {
    return acceptingStepName;
  }

  /**
   * @param acceptingStepName The acceptingStepName to set.
   */
  public void setAcceptingStepName( String acceptingStepName ) {
    this.acceptingStepName = acceptingStepName;
  }

  /**
   * @return Returns the filenameField.
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField The filenameField to set.
   */
  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  /**
   * @return Returns the includeFilename.
   */
  public boolean includeFilename() {
    return includeFilename;
  }

  /**
   * @param includeFilename The includeFilename to set.
   */
  public void setIncludeFilename( boolean includeFilename ) {
    this.includeFilename = includeFilename;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    XBaseInputMeta retval = (XBaseInputMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      dbfFileName = XMLHandler.getTagValue( stepnode, "file_dbf" );
      rowLimit = Const.toInt( XMLHandler.getTagValue( stepnode, "limit" ), 0 );
      rowNrAdded = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "add_rownr" ) );
      rowNrField = XMLHandler.getTagValue( stepnode, "field_rownr" );

      includeFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "include" ) );
      filenameField = XMLHandler.getTagValue( stepnode, "include_field" );
      charactersetName = XMLHandler.getTagValue( stepnode, "charset_name" );

      acceptingFilenames = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "accept_filenames" ) );
      acceptingField = XMLHandler.getTagValue( stepnode, "accept_field" );
      acceptingStepName = XMLHandler.getTagValue( stepnode, "accept_stepname" );

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "XBaseInputMeta.Exception.UnableToReadStepInformationFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    dbfFileName = null;
    rowLimit = 0;
    rowNrAdded = false;
    rowNrField = null;
  }

  public String getLookupStepname() {
    if ( acceptingFilenames && acceptingStep != null && !Utils.isEmpty( acceptingStep.getName() ) ) {
      return acceptingStep.getName();
    }
    return null;
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    acceptingStep = StepMeta.findStep( steps, acceptingStepName );
  }

  public String[] getInfoSteps() {
    if ( acceptingFilenames && acceptingStep != null ) {
      return new String[] { acceptingStep.getName() };
    }
    return null;
  }

  public RowMetaInterface getOutputFields( FileInputList files, String name ) throws HopStepException {
    RowMetaInterface rowMeta = new RowMeta();

    // Take the first file to determine what the layout is...
    //
    XBase xbi = null;
    try {
      xbi = new XBase( getLog(), HopVFS.getInputStream( files.getFile( 0 ) ) );
      xbi.setDbfFile( files.getFile( 0 ).getName().getURI() );
      xbi.open();
      RowMetaInterface add = xbi.getFields();
      for ( int i = 0; i < add.size(); i++ ) {
        ValueMetaInterface v = add.getValueMeta( i );
        v.setOrigin( name );
      }
      rowMeta.addRowMeta( add );
    } catch ( Exception ke ) {
      throw new HopStepException( BaseMessages.getString(
        PKG, "XBaseInputMeta.Exception.UnableToReadMetaDataFromXBaseFile" ), ke );
    } finally {
      if ( xbi != null ) {
        xbi.close();
      }
    }

    if ( rowNrAdded && rowNrField != null && rowNrField.length() > 0 ) {
      ValueMetaInterface rnr = new ValueMetaInteger( rowNrField );
      rnr.setOrigin( name );
      rowMeta.addValueMeta( rnr );
    }

    if ( includeFilename ) {
      ValueMetaInterface v = new ValueMetaString( filenameField );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      rowMeta.addValueMeta( v );
    }
    return rowMeta;
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    FileInputList fileList = getTextFileList( space );
    if ( fileList.nrOfFiles() == 0 ) {
      throw new HopStepException( BaseMessages
        .getString( PKG, "XBaseInputMeta.Exception.NoFilesFoundToProcess" ) );
    }

    row.addRowMeta( getOutputFields( fileList, name ) );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "file_dbf", dbfFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( "limit", rowLimit ) );
    retval.append( "    " + XMLHandler.addTagValue( "add_rownr", rowNrAdded ) );
    retval.append( "    " + XMLHandler.addTagValue( "field_rownr", rowNrField ) );

    retval.append( "    " + XMLHandler.addTagValue( "include", includeFilename ) );
    retval.append( "    " + XMLHandler.addTagValue( "include_field", filenameField ) );
    retval.append( "    " + XMLHandler.addTagValue( "charset_name", charactersetName ) );

    retval.append( "    " + XMLHandler.addTagValue( "accept_filenames", acceptingFilenames ) );
    retval.append( "    " + XMLHandler.addTagValue( "accept_field", acceptingField ) );
    if ( ( acceptingStepName == null ) && ( acceptingStep != null ) ) {
      acceptingStepName = acceptingStep.getName();
    }
    retval.append( "    "
      + XMLHandler.addTagValue( "accept_stepname", acceptingStepName ) );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {

    CheckResult cr;

    if ( dbfFileName == null ) {
      if ( isAcceptingFilenames() ) {
        if ( Utils.isEmpty( getAcceptingStepName() ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "XBaseInput.Log.Error.InvalidAcceptingStepName" ), stepMeta );
          remarks.add( cr );
        }

        if ( Utils.isEmpty( getAcceptingField() ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "XBaseInput.Log.Error.InvalidAcceptingFieldName" ), stepMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.PleaseSelectFileToUse" ), stepMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "XBaseInputMeta.Remark.FileToUseIsSpecified" ), stepMeta );
      remarks.add( cr );

      XBase xbi = new XBase( getLog(), pipelineMeta.environmentSubstitute( dbfFileName ) );
      try {
        xbi.open();
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.FileExistsAndCanBeOpened" ), stepMeta );
        remarks.add( cr );

        RowMetaInterface r = xbi.getFields();

        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, r.size()
            + BaseMessages.getString( PKG, "XBaseInputMeta.Remark.OutputFieldsCouldBeDetermined" ), stepMeta );
        remarks.add( cr );
      } catch ( HopException ke ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.NoFieldsCouldBeFoundInFileBecauseOfError" )
            + Const.CR + ke.getMessage(), stepMeta );
        remarks.add( cr );
      } finally {
        xbi.close();
      }
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new XBaseInput( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new XBaseInputData();
  }

  public String[] getFilePaths( VariableSpace space ) {
    return FileInputList.createFilePathList(
      space, new String[] { dbfFileName }, new String[] { null }, new String[] { null }, new String[] { "N" } );
  }

  public FileInputList getTextFileList( VariableSpace space ) {
    return FileInputList.createFileList(
      space, new String[] { dbfFileName }, new String[] { null }, new String[] { null }, new String[] { "N" } );
  }

  /**
   * @return the charactersetName
   */
  public String getCharactersetName() {
    return charactersetName;
  }

  /**
   * @param charactersetName the charactersetName to set
   */
  public void setCharactersetName( String charactersetName ) {
    this.charactersetName = charactersetName;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files relatively. So
   * what this does is turn the name of files into absolute paths OR it simply includes the resource in the ZIP file.
   * For now, we'll simply turn it into an absolute path and pray that the file is on a shared drive or something like
   * that.
   *
   * @param space                   the variable space to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous steps, forget about this!
      //
      if ( !acceptingFilenames ) {
        // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.dbf
        // To : /home/matt/test/files/foo/bar.dbf
        //
        FileObject fileObject = HopVFS.getFileObject( space.environmentSubstitute( dbfFileName ), space );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          dbfFileName = resourceNamingInterface.nameResource( fileObject, space, true );

          return dbfFileName;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

}
