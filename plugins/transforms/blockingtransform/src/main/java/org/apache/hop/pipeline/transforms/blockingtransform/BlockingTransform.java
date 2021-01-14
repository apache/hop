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

package org.apache.hop.pipeline.transforms.blockingtransform;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A transform that blocks throughput until the input ends, then it will either output the last row or the complete input.
 */
public class BlockingTransform extends BaseTransform<BlockingTransformMeta, BlockingTransformData> implements ITransform<BlockingTransformMeta, BlockingTransformData> {

  private static final Class<?> PKG = BlockingTransformMeta.class; // For Translator

  private Object[] lastRow;

  public BlockingTransform( TransformMeta transformMeta, BlockingTransformMeta meta, BlockingTransformData data, int copyNr, PipelineMeta pipelineMeta,
                            Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  private boolean addBuffer( IRowMeta rowMeta, Object[] r ) {
    if ( r != null ) {
      data.buffer.add( r ); // Save row
    }

    // Time to write to disk: buffer in core is full!
    if ( data.buffer.size() == meta.getCacheSize() // Buffer is full: dump to disk
      || ( data.files.size() > 0 && r == null && data.buffer.size() > 0 ) // No more records: join from disk
    ) {
      // Then write them to disk...
      DataOutputStream dos;
      GZIPOutputStream gzos;
      int p;

      try {
        FileObject fileObject =
          HopVfs.createTempFile( meta.getPrefix(), ".tmp", resolve( meta.getDirectory() ) );

        data.files.add( fileObject ); // Remember the files!
        OutputStream outputStream = HopVfs.getOutputStream( fileObject, false );
        if ( meta.getCompress() ) {
          gzos = new GZIPOutputStream( new BufferedOutputStream( outputStream ) );
          dos = new DataOutputStream( gzos );
        } else {
          dos = new DataOutputStream( outputStream );
          gzos = null;
        }

        // How many records do we have?
        dos.writeInt( data.buffer.size() );

        for ( p = 0; p < data.buffer.size(); p++ ) {
          // Just write the data, nothing else
          rowMeta.writeData( dos, data.buffer.get( p ) );
        }
        // Close temp-file
        dos.close(); // close data stream
        if ( gzos != null ) {
          gzos.close(); // close gzip stream
        }
        outputStream.close(); // close file stream
      } catch ( Exception e ) {
        logError( "Error processing tmp-file: " + e.toString() );
        return false;
      }

      data.buffer.clear();
    }

    return true;
  }

  private Object[] getBuffer() {
    Object[] retval;

    // Open all files at once and read one row from each file...
    if ( data.files.size() > 0 && ( data.dis.size() == 0 || data.fis.size() == 0 ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "BlockingTransform.Log.Openfiles" ) );
      }

      try {
        FileObject fileObject = data.files.get( 0 );
        String filename = HopVfs.getFilename( fileObject );
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "BlockingTransform.Log.Openfilename1" ) + filename
            + BaseMessages.getString( PKG, "BlockingTransform.Log.Openfilename2" ) );
        }
        InputStream fi = HopVfs.getInputStream( fileObject );
        DataInputStream di;
        data.fis.add( fi );
        if ( meta.getCompress() ) {
          GZIPInputStream gzfi = new GZIPInputStream( new BufferedInputStream( fi ) );
          di = new DataInputStream( gzfi );
          data.gzis.add( gzfi );
        } else {
          di = new DataInputStream( fi );
        }
        data.dis.add( di );

        // How long is the buffer?
        int buffersize = di.readInt();

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "BlockingTransform.Log.BufferSize1" ) + filename
            + BaseMessages.getString( PKG, "BlockingTransform.Log.BufferSize2" ) + buffersize + " "
            + BaseMessages.getString( PKG, "BlockingTransform.Log.BufferSize3" ) );
        }

        if ( buffersize > 0 ) {
          // Read a row from temp-file
          data.rowbuffer.add( data.outputRowMeta.readData( di ) );
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "BlockingTransformMeta.ErrorReadingFile" ) + e.toString() );
        logError( Const.getStackTracker( e ) );
      }
    }

    if ( data.files.size() == 0 ) {
      if ( data.buffer.size() > 0 ) {
        retval = data.buffer.get( 0 );
        data.buffer.remove( 0 );
      } else {
        retval = null;
      }
    } else {
      if ( data.rowbuffer.size() == 0 ) {
        retval = null;
      } else {
        retval = data.rowbuffer.get( 0 );

        data.rowbuffer.remove( 0 );

        // now get another
        FileObject file = data.files.get( 0 );
        DataInputStream di = data.dis.get( 0 );
        InputStream fi = data.fis.get( 0 );
        GZIPInputStream gzfi = ( meta.getCompress() ) ? data.gzis.get( 0 ) : null;

        try {
          data.rowbuffer.add( 0, data.outputRowMeta.readData( di ) );
        } catch ( SocketTimeoutException e ) {
          logError( BaseMessages.getString( PKG, "System.Log.UnexpectedError" ) + " : " + e.toString() );
          logError( Const.getStackTracker( e ) );
          setErrors( 1 );
          stopAll();
        } catch ( HopFileException fe ) {
          // empty file or EOF mostly
          try {
            di.close();
            fi.close();
            if ( gzfi != null ) {
              gzfi.close();
            }
            file.delete();
          } catch ( IOException e ) {
            logError( BaseMessages.getString( PKG, "BlockingTransformMeta.UnableDeleteFile" ) + file.toString() );
            setErrors( 1 );
            stopAll();
            return null;
          }

          data.files.remove( 0 );
          data.dis.remove( 0 );
          data.fis.remove( 0 );
          if ( gzfi != null ) {
            data.gzis.remove( 0 );
          }
        }
      }
    }
    return retval;
  }

  @Override
  public void dispose() {
    if ( ( data.dis != null ) && ( data.dis.size() > 0 ) ) {
      for ( DataInputStream is : data.dis ) {
        BaseTransform.closeQuietly( is );
      }
    }
    // remove temp files
    for ( int f = 0; f < data.files.size(); f++ ) {
      FileObject fileToDelete = data.files.get( f );
      try {
        if ( fileToDelete != null && fileToDelete.exists() ) {
          fileToDelete.delete();
        }
      } catch ( FileSystemException e ) {
        logError( e.getLocalizedMessage(), e );
      }
    }
    super.dispose();
  }

  @Override
  public boolean init( ) {

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

  @Override
  public boolean processRow() throws HopException {

    boolean err = true;
    Object[] r = getRow(); // Get row from input rowset & set row busy!

    // initialize
    if ( first && r != null ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
    }

    if ( !meta.isPassAllRows() ) {
      if ( r == null ) {
        // no more input to be expected...
        if ( lastRow != null ) {
          putRow( data.outputRowMeta, lastRow );
        }
        setOutputDone();
        return false;
      }

      lastRow = r;
      return true;
    } else {
      // The mode in which we pass all rows to the output.
      err = addBuffer( getInputRowMeta(), r );
      if ( !err ) {
        setOutputDone(); // signal receiver we're finished.
        return false;
      }

      if ( r == null ) {
        // no more input to be expected...
        // Now we can start the output!
        r = getBuffer();
        while ( r != null && !isStopped() ) {
          if ( log.isRowLevel() ) {
            logRowlevel( "Read row: " + getInputRowMeta().getString( r ) );
          }

          putRow( data.outputRowMeta, r ); // copy row to possible alternate rowset(s).

          r = getBuffer();
        }

        setOutputDone(); // signal receiver we're finished.
        return false;
      }

      return true;
    }
  }

}
