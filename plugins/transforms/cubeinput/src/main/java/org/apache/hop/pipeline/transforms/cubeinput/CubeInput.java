/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.cubeinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.zip.GZIPInputStream;

public class CubeInput extends BaseTransform<CubeInputMeta, CubeInputData> implements ITransform<CubeInputMeta, CubeInputData> {

  private static final Class<?> PKG = CubeInputMeta.class; // For Translator

  private int realRowLimit;

  public CubeInput( TransformMeta transformMeta, CubeInputMeta meta, CubeInputData data, int copyNr, PipelineMeta pipelineMeta,
                    Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {

    if ( first ) {
      first = false;
      realRowLimit = Const.toInt( resolve( meta.getRowLimit() ), 0 );
    }

    try {
      Object[] r = data.meta.readData( data.dis );
      putRow( data.meta, r ); // fill the rowset(s). (sleeps if full)
      incrementLinesInput();

      if ( realRowLimit > 0 && getLinesInput() >= realRowLimit ) { // finished!
        setOutputDone();
        return false;
      }
    } catch ( HopEofException eof ) {
      setOutputDone();
      return false;
    } catch ( SocketTimeoutException e ) {
      throw new HopException( e ); // shouldn't happen on files
    }

    if ( checkFeedback( getLinesInput() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "CubeInput.Log.LineNumber" ) + getLinesInput() );
      }
    }

    return true;
  }

  @Override public boolean init(){

    if ( super.init() ) {
      try {
        String filename = resolve( meta.getFilename() );

        // Add filename to result filenames ?
        if ( meta.isAddResultFile() ) {
          ResultFile resultFile =
            new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( filename ),
              getPipelineMeta().getName(), toString() );
          resultFile.setComment( "File was read by a Cube Input transform" );
          addResultFile( resultFile );
        }

        data.fis = HopVfs.getInputStream( filename );
        data.zip = new GZIPInputStream( data.fis );
        data.dis = new DataInputStream( data.zip );

        try {
          data.meta = new RowMeta( data.dis );
          return true;
        } catch ( HopFileException kfe ) {
          logError( BaseMessages.getString( PKG, "CubeInput.Log.UnableToReadMetadata" ), kfe );
          return false;
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "CubeInput.Log.ErrorReadingFromDataCube" ), e );
      }
    }
    return false;
  }

  @Override public void dispose(){
    try {
      if ( data.dis != null ) {
        data.dis.close();
        data.dis = null;
      }
      if ( data.zip != null ) {
        data.zip.close();
        data.zip = null;
      }
      if ( data.fis != null ) {
        data.fis.close();
        data.fis = null;
      }
    } catch ( IOException e ) {
      logError( BaseMessages.getString( PKG, "CubeInput.Log.ErrorClosingCube" ) + e.toString() );
      setErrors( 1 );
      stopAll();
    }

    super.dispose();
  }
}
