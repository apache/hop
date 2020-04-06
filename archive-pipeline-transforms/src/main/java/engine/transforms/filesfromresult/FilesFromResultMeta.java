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

package org.apache.hop.pipeline.transforms.filesfromresult;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class FilesFromResultMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = FilesFromResult.class; // for i18n purposes, needed by Translator!!

  public FilesFromResultMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node transformNode ) {
  }

  public void setDefault() {
  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    // Add the fields from a ResultFile
    try {
      ResultFile resultFile =
        new ResultFile(
          ResultFile.FILE_TYPE_GENERAL, HopVFS.getFileObject( "foo.bar", variables ), "parentOrigin", "origin" );
      RowMetaAndData add = resultFile.getRow();

      // Set the origin on the fields...
      for ( int i = 0; i < add.size(); i++ ) {
        add.getValueMeta( i ).setOrigin( name );
      }
      r.addRowMeta( add.getRowMeta() );
    } catch ( HopFileException e ) {
      throw new HopTransformException( e );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FilesFromResultMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilesFromResultMeta.CheckResult.NoInputReceivedError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new FilesFromResult( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new FilesFromResultData();
  }

}
