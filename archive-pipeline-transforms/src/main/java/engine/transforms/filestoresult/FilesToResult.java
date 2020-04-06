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

package org.apache.hop.pipeline.transforms.filestoresult;

import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Writes filenames to a next job entry in a Job
 *
 * @author matt
 * @since 26-may-2006
 */
public class FilesToResult extends BaseTransform implements ITransform {
  private static Class<?> PKG = FilesToResultMeta.class; // for i18n purposes, needed by Translator!!

  private FilesToResultMeta meta;

  private FilesToResultData data;

  public FilesToResult( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (FilesToResultMeta) smi;
    data = (FilesToResultData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      for ( ResultFile resultFile : data.filenames ) {
        addResultFile( resultFile );
      }
      logBasic( BaseMessages.getString( PKG, "FilesToResult.Log.AddedNrOfFiles", String.valueOf( data.filenames
        .size() ) ) );
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.filenameIndex = getInputRowMeta().indexOfValue( meta.getFilenameField() );

      if ( data.filenameIndex < 0 ) {
        logError( BaseMessages.getString( PKG, "FilesToResult.Log.CouldNotFindField", meta.getFilenameField() ) );
        setErrors( 1 );
        stopAll();
        return false;
      }
    }

    // OK, get the filename field from the row
    String filename = getInputRowMeta().getString( r, data.filenameIndex );

    try {
      ResultFile resultFile =
        new ResultFile( meta.getFileType(), HopVFS.getFileObject( filename, getPipelineMeta() ), getPipeline()
          .getName(), getTransformName() );

      // Add all rows to rows buffer...
      data.filenames.add( resultFile );
    } catch ( Exception e ) {
      throw new HopException( e );
    }

    // Copy to any possible next transforms...
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    putRow( data.outputRowMeta, r ); // copy row to possible alternate
    // rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "FilesToResult.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init() {
    meta = (FilesToResultMeta) smi;
    data = (FilesToResultData) sdi;

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
