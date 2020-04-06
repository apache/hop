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

package org.apache.hop.pipeline.transforms.cubeoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
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
 * Created on 4-apr-2003
 *
 */
public class CubeOutputMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = CubeOutputMeta.class; // for i18n purposes, needed by Translator!!

  private String filename;
  /**
   * Flag: add the filenames to result filenames
   */
  private boolean addToResultFilenames;

  /**
   * Flag : Do not open new file when pipeline start
   */
  private boolean doNotOpenNewFileInit;

  public CubeOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  /**
   * @param filename The filename to set.
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @return Returns the filename.
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean isAddToResultFiles() {
    return addToResultFilenames;
  }

  /**
   * @param addtoresultfilenamesin The addtoresultfilenames to set.
   */
  public void setAddToResultFiles( boolean addtoresultfilenamesin ) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  /**
   * @return Returns the "do not open new file at init" flag.
   */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /**
   * @param doNotOpenNewFileInit The "do not open new file at init" flag to set.
   */
  public void setDoNotOpenNewFileInit( boolean doNotOpenNewFileInit ) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  public Object clone() {
    CubeOutputMeta retval = (CubeOutputMeta) super.clone();

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      filename = XMLHandler.getTagValue( transformNode, "file", "name" );
      addToResultFilenames =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_to_result_filenames" ) );
      doNotOpenNewFileInit =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "do_not_open_newfile_init" ) );

    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "CubeOutputMeta.Exception.UnableToLoadTransformMeta" ), e );
    }
  }

  public void setDefault() {
    filename = "file.cube";
    addToResultFilenames = false;
    doNotOpenNewFileInit = false;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <file>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "name", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_to_result_filenames", addToResultFilenames ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "do_not_open_newfile_init", doNotOpenNewFileInit ) );

    retval.append( "    </file>" ).append( Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CubeOutputMeta.CheckResult.ReceivingFields", String.valueOf( prev.size() ) ), transformMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResult.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "CubeOutputMeta.CheckResult.FileSpecificationsNotChecked" ), transformMeta );
    remarks.add( cr );
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new CubeOutput( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new CubeOutputData();
  }

  /**
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources( iVariables variables, Map<String, ResourceDefinition> definitions,
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
}
