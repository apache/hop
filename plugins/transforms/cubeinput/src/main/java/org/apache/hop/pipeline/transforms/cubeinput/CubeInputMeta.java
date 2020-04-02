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

package org.apache.hop.pipeline.transforms.cubeinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.w3c.dom.Node;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/*
 * Created on 2-jun-2003
 *
 */
@Transform(
        id = "CubeInput",
        image = "ui/images/CIP.svg",
        i18nPackageName = "i18n:org.apache.hop.pipeline.transforms.cubeinput",
        name = "BaseTransform.TypeLongDesc.CubeInput",
        description = "BaseTransform.TypeTooltipDesc.CubeInput",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
        documentationUrl = ""
)
public class CubeInputMeta extends BaseTransformMeta implements ITransformMeta<CubeInput, CubeInputData> {

  private static Class<?> PKG = CubeInputMeta.class; // for i18n purposes, needed by Translator!!

  private String filename;
  private String rowLimit;
  private boolean addfilenameresult;

  public CubeInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  /**
   * @return Returns the filename.
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set.
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  @Deprecated
  public void setRowLimit( int rowLimit ) {
    this.rowLimit = String.valueOf( rowLimit );
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( String rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the rowLimit.
   */
  public String getRowLimit() {
    return rowLimit;
  }

  /**
   * @return Returns the addfilenameresult.
   */
  public boolean isAddResultFile() {
    return addfilenameresult;
  }

  /**
   * @param addfilenameresult The addfilenameresult to set.
   */
  public void setAddResultFile( boolean addfilenameresult ) {
    this.addfilenameresult = addfilenameresult;
  }

  @Override public Object clone() {
    CubeInputMeta retval = (CubeInputMeta) super.clone();
    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      filename = XMLHandler.getTagValue( transformNode, "file", "name" );
      rowLimit = XMLHandler.getTagValue( transformNode, "limit" );
      addfilenameresult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addfilenameresult" ) );

    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "CubeInputMeta.Exception.UnableToLoadTransformMeta" ), e );
    }
  }

  @Override public void setDefault() {
    filename = "file";
    rowLimit = "0";
    addfilenameresult = false;
  }

  @Override public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                                   IVariables variables, IMetaStore metaStore ) throws HopTransformException {
    GZIPInputStream fis = null;
    DataInputStream dis = null;
    try {
      InputStream is = HopVFS.getInputStream( variables.environmentSubstitute( filename ), variables );
      fis = new GZIPInputStream( is );
      dis = new DataInputStream( fis );

      IRowMeta add = new RowMeta( dis );
      for ( int i = 0; i < add.size(); i++ ) {
        add.getValueMeta( i ).setOrigin( name );
      }
      r.mergeRowMeta( add );
    } catch ( HopFileException kfe ) {
      throw new HopTransformException(
        BaseMessages.getString( PKG, "CubeInputMeta.Exception.UnableToReadMetaData" ), kfe );
    } catch ( IOException e ) {
      throw new HopTransformException( BaseMessages.getString(
        PKG, "CubeInputMeta.Exception.ErrorOpeningOrReadingCubeFile" ), e );
    } finally {
      try {
        if ( fis != null ) {
          fis.close();
        }
        if ( dis != null ) {
          dis.close();
        }
      } catch ( IOException ioe ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "CubeInputMeta.Exception.UnableToCloseCubeFile" ), ioe );
      }
    }
  }

  @Override public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <file>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "name", filename ) );
    retval.append( "    </file>" ).append( Const.CR );
    retval.append( "    " ).append( XMLHandler.addTagValue( "limit", rowLimit ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "addfilenameresult", addfilenameresult ) );

    return retval.toString();
  }

  @Override public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                               IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                               IMetaStore metaStore ) {
    CheckResult cr;

    cr =
      new CheckResult( CheckResult.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "CubeInputMeta.CheckResult.FileSpecificationsNotChecked" ), transformMeta );
    remarks.add( cr );
  }

  @Override public CubeInput createTransform( TransformMeta transformMeta, CubeInputData iTransformData, int cnr, PipelineMeta tr,
                                              Pipeline pipeline ) {
    return new CubeInput( transformMeta, iTransformData, cnr, tr, pipeline );
  }

  @Override public CubeInputData getTransformData() {
    return new CubeInputData();
  }

  /**
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  @Override public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                           IResourceNaming iResourceNaming, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To : /home/matt/test/files/foo/bar.data
      //
      FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( filename ), variables );

      // If the file doesn't exist, forget about this effort too!
      //
      if ( fileObject.exists() ) {
        // Convert to an absolute path...
        //
        filename = iResourceNaming.nameResource( fileObject, variables, true );

        return filename;
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override
  public String getDialogClassName(){
    return CubeInputDialog.class.getName();
  }

}
