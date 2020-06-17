/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.xbaseinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/*
 * Created on 2-jun-2003
 *
 */

public class XBaseInputMeta extends BaseTransformMeta implements ITransform {
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
   * The transformName to accept filenames from
   */
  private String acceptingTransformName;

  /**
   * The transform to accept filenames from
   */
  private TransformMeta acceptingTransform;

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
    super(); // allocate BaseTransformMeta
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
   * @return Returns the acceptingTransform.
   */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /**
   * @param acceptingTransform The acceptingTransform to set.
   */
  public void setAcceptingTransform( TransformMeta acceptingTransform ) {
    this.acceptingTransform = acceptingTransform;
  }

  /**
   * @return Returns the acceptingTransformName.
   */
  public String getAcceptingTransformName() {
    return acceptingTransformName;
  }

  /**
   * @param acceptingTransformName The acceptingTransformName to set.
   */
  public void setAcceptingTransformName( String acceptingTransformName ) {
    this.acceptingTransformName = acceptingTransformName;
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
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    XBaseInputMeta retval = (XBaseInputMeta) super.clone();
    return retval;
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      dbfFileName = XmlHandler.getTagValue( transformNode, "file_dbf" );
      rowLimit = Const.toInt( XmlHandler.getTagValue( transformNode, "limit" ), 0 );
      rowNrAdded = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "add_rownr" ) );
      rowNrField = XmlHandler.getTagValue( transformNode, "field_rownr" );

      includeFilename = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "include" ) );
      filenameField = XmlHandler.getTagValue( transformNode, "include_field" );
      charactersetName = XmlHandler.getTagValue( transformNode, "charset_name" );

      acceptingFilenames = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "accept_filenames" ) );
      acceptingField = XmlHandler.getTagValue( transformNode, "accept_field" );
      acceptingTransformName = XmlHandler.getTagValue( transformNode, "accept_transform_name" );

    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "XBaseInputMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    dbfFileName = null;
    rowLimit = 0;
    rowNrAdded = false;
    rowNrField = null;
  }

  public String getLookupTransformName() {
    if ( acceptingFilenames && acceptingTransform != null && !Utils.isEmpty( acceptingTransform.getName() ) ) {
      return acceptingTransform.getName();
    }
    return null;
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    acceptingTransform = TransformMeta.findTransform( transforms, acceptingTransformName );
  }

  public String[] getInfoTransforms() {
    if ( acceptingFilenames && acceptingTransform != null ) {
      return new String[] { acceptingTransform.getName() };
    }
    return null;
  }

  public IRowMeta getOutputFields( FileInputList files, String name ) throws HopTransformException {
    IRowMeta rowMeta = new RowMeta();

    // Take the first file to determine what the layout is...
    //
    XBase xbi = null;
    try {
      xbi = new XBase( getLog(), HopVFS.getInputStream( files.getFile( 0 ) ) );
      xbi.setDbfFile( files.getFile( 0 ).getName().getURI() );
      xbi.open();
      IRowMeta add = xbi.getFields();
      for ( int i = 0; i < add.size(); i++ ) {
        IValueMeta v = add.getValueMeta( i );
        v.setOrigin( name );
      }
      rowMeta.addRowMeta( add );
    } catch ( Exception ke ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "XBaseInputMeta.Exception.UnableToReadMetaDataFromXBaseFile" ), ke );
    } finally {
      if ( xbi != null ) {
        xbi.close();
      }
    }

    if ( rowNrAdded && rowNrField != null && rowNrField.length() > 0 ) {
      IValueMeta rnr = new ValueMetaInteger( rowNrField );
      rnr.setOrigin( name );
      rowMeta.addValueMeta( rnr );
    }

    if ( includeFilename ) {
      IValueMeta v = new ValueMetaString( filenameField );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      rowMeta.addValueMeta( v );
    }
    return rowMeta;
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    FileInputList fileList = getTextFileList( variables );
    if ( fileList.nrOfFiles() == 0 ) {
      throw new HopTransformException( BaseMessages
        .getString( PKG, "XBaseInputMeta.Exception.NoFilesFoundToProcess" ) );
    }

    row.addRowMeta( getOutputFields( fileList, name ) );
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XmlHandler.addTagValue( "file_dbf", dbfFileName ) );
    retval.append( "    " + XmlHandler.addTagValue( "limit", rowLimit ) );
    retval.append( "    " + XmlHandler.addTagValue( "add_rownr", rowNrAdded ) );
    retval.append( "    " + XmlHandler.addTagValue( "field_rownr", rowNrField ) );

    retval.append( "    " + XmlHandler.addTagValue( "include", includeFilename ) );
    retval.append( "    " + XmlHandler.addTagValue( "include_field", filenameField ) );
    retval.append( "    " + XmlHandler.addTagValue( "charset_name", charactersetName ) );

    retval.append( "    " + XmlHandler.addTagValue( "accept_filenames", acceptingFilenames ) );
    retval.append( "    " + XmlHandler.addTagValue( "accept_field", acceptingField ) );
    if ( ( acceptingTransformName == null ) && ( acceptingTransform != null ) ) {
      acceptingTransformName = acceptingTransform.getName();
    }
    retval.append( "    "
      + XmlHandler.addTagValue( "accept_transform_name", acceptingTransformName ) );

    return retval.toString();
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {

    CheckResult cr;

    if ( dbfFileName == null ) {
      if ( isAcceptingFilenames() ) {
        if ( Utils.isEmpty( getAcceptingTransformName() ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "XBaseInput.Log.Error.InvalidAcceptingTransformName" ), transformMeta );
          remarks.add( cr );
        }

        if ( Utils.isEmpty( getAcceptingField() ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "XBaseInput.Log.Error.InvalidAcceptingFieldName" ), transformMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.PleaseSelectFileToUse" ), transformMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "XBaseInputMeta.Remark.FileToUseIsSpecified" ), transformMeta );
      remarks.add( cr );

      XBase xbi = new XBase( getLog(), pipelineMeta.environmentSubstitute( dbfFileName ) );
      try {
        xbi.open();
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.FileExistsAndCanBeOpened" ), transformMeta );
        remarks.add( cr );

        IRowMeta r = xbi.getFields();

        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, r.size()
            + BaseMessages.getString( PKG, "XBaseInputMeta.Remark.OutputFieldsCouldBeDetermined" ), transformMeta );
        remarks.add( cr );
      } catch ( HopException ke ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "XBaseInputMeta.Remark.NoFieldsCouldBeFoundInFileBecauseOfError" )
            + Const.CR + ke.getMessage(), transformMeta );
        remarks.add( cr );
      } finally {
        xbi.close();
      }
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new XBaseInput( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new XBaseInputData();
  }

  public String[] getFilePaths( IVariables variables ) {
    return FileInputList.createFilePathList(
      variables, new String[] { dbfFileName }, new String[] { null }, new String[] { null }, new String[] { "N" } );
  }

  public FileInputList getTextFileList( IVariables variables ) {
    return FileInputList.createFileList(
      variables, new String[] { dbfFileName }, new String[] { null }, new String[] { null }, new String[] { "N" } );
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
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider               the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IHopMetadataProvider metadataProvider ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( !acceptingFilenames ) {
        // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.dbf
        // To : /home/matt/test/files/foo/bar.dbf
        //
        FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( dbfFileName ), variables );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          dbfFileName = iResourceNaming.nameResource( fileObject, variables, true );

          return dbfFileName;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

}
