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

package org.apache.hop.pipeline.transforms.joinrows;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs a cartesian product between 2 or more input streams.
 *
 * @author Matt
 * @since 29-apr-2003
 */
public class JoinRows extends BaseTransform<JoinRowsMeta, JoinRowsData> implements ITransform<JoinRowsMeta, JoinRowsData> {
  private static final Class<?> PKG = JoinRowsMeta.class; // For Translator

  public JoinRows(TransformMeta transformMeta, JoinRowsMeta meta, JoinRowsData data, int copyNr, PipelineMeta pipelineMeta,
                  Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /*
   * Allocate input streams and create the temporary files...
   */
  @SuppressWarnings( "unchecked" )
  public void initialize() throws HopException {
    // Since we haven't called getRow() yet we need to wait until we have all input row sets available to us.
    //
    //openRemoteInputTransformSocketsOnce();

    try {
      // Start with the caching of the data, write later...
      data.caching = true;

      // Start at file 1, skip 0 for speed!
      data.filenr = 1;

      // See if a main transform is supplied: in that case move the corresponding rowset to position 0
      swapFirstInputRowSetIfExists( meta.getMainTransformName() );

      List<IRowSet> inputRowSets = getInputRowSets();
      int rowSetsSize = inputRowSets.size();

      // ** INPUT SIDE **
      data.file = new File[ rowSetsSize ];
      data.fileInputStream = new FileInputStream[ rowSetsSize ];
      data.dataInputStream = new DataInputStream[ rowSetsSize ];
      data.size = new int[ rowSetsSize ];
      data.fileRowMeta = new IRowMeta[ rowSetsSize ];
      data.joinrow = new Object[ rowSetsSize ][];
      data.rs = new IRowSet[ rowSetsSize ];
      data.cache = new List[ rowSetsSize ];
      data.position = new int[ rowSetsSize ];
      data.fileOutputStream = new FileOutputStream[ rowSetsSize ];
      data.dataOutputStream = new DataOutputStream[ rowSetsSize ];
      data.restart = new boolean[ rowSetsSize ];

      for ( int i = 1; i < rowSetsSize; i++ ) {
        String directoryName = resolve( meta.getDirectory() );
        File file = null;
        if ( directoryName != null ) {
          file = new File( directoryName );
        }
        data.file[ i ] = File.createTempFile( meta.getPrefix(), ".tmp", file );

        data.size[ i ] = 0;
        data.rs[ i ] = inputRowSets.get( i );
        data.cache[ i ] = null;
        // data.row[i] = null;
        data.position[ i ] = 0;

        data.dataInputStream[ i ] = null;
        data.dataOutputStream[ i ] = null;

        data.joinrow[ i ] = null;
        data.restart[ i ] = false;
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JoinRows.Log.ErrorCreatingTemporaryFiles" ), e );
    }
  }

  /**
   * Get a row of data from the indicated rowset or buffer (memory/disk)
   *
   * @param filenr The rowset or buffer to read a row from
   * @return a row of data
   * @throws HopException in case something goes wrong
   */
  public Object[] getRowData( int filenr ) throws HopException {
    data.restart[ filenr ] = false;

    Object[] rowData = null;

    // Do we read from the first rowset or a file?
    if ( filenr == 0 ) {
      // Rowset 0:
      IRowSet rowSet = getFirstInputRowSet();
      rowData = getRowFrom( rowSet );
      if ( rowData != null ) {
        data.fileRowMeta[ 0 ] = rowSet.getRowMeta();
      }

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "JoinRows.Log.ReadRowFromStream" )
          + ( rowData == null ? "<null>" : data.fileRowMeta[ 0 ].getString( rowData ) ) );
      }
    } else {
      if ( data.cache[ filenr ] == null ) {
        // See if we need to open the file?
        if ( data.dataInputStream[ filenr ] == null ) {
          try {
            data.fileInputStream[ filenr ] = new FileInputStream( data.file[ filenr ] );
            data.dataInputStream[ filenr ] = new DataInputStream( data.fileInputStream[ filenr ] );
          } catch ( FileNotFoundException fnfe ) {
            logError( BaseMessages.getString( PKG, "JoinRows.Log.UnableToFindOrOpenTemporaryFile" )
              + data.file[ filenr ] + "] : " + fnfe.toString() );
            setErrors( 1 );
            stopAll();
            return null;
          }
        }

        // Read a row from the temporary file

        if ( data.size[ filenr ] == 0 ) {
          if ( log.isBasic() ) {
            logBasic( BaseMessages.getString( PKG, "JoinRows.Log.NoRowsComingFromTransform" )
              + data.rs[ filenr ].getOriginTransformName() + "]" );
          }
          return null;
        }

        try {
          rowData = data.fileRowMeta[ filenr ].readData( data.dataInputStream[ filenr ] );
        } catch ( HopFileException e ) {
          logError( BaseMessages.getString( PKG, "JoinRows.Log.UnableToReadDataFromTempFile" )
            + filenr + " [" + data.file[ filenr ] + "]" );
          setErrors( 1 );
          stopAll();
          return null;
        } catch ( SocketTimeoutException e ) {
          logError( BaseMessages.getString( PKG, "JoinRows.Log.UnableToReadDataFromTempFile" )
            + filenr + " [" + data.file[ filenr ] + "]" );
          setErrors( 1 );
          stopAll();
          return null;
        }
        if ( log.isRowLevel() ) {
          logRowlevel( BaseMessages.getString( PKG, "JoinRows.Log.ReadRowFromFile" )
            + filenr + " : " + data.fileRowMeta[ filenr ].getString( rowData ) );
        }

        data.position[ filenr ]++;

        // If the file is at the end, close it.
        // The file will then be re-opened if needed later on.
        if ( data.position[ filenr ] >= data.size[ filenr ] ) {
          try {
            data.dataInputStream[ filenr ].close();
            data.fileInputStream[ filenr ].close();

            data.dataInputStream[ filenr ] = null;
            data.fileInputStream[ filenr ] = null;

            data.position[ filenr ] = 0;
            data.restart[ filenr ] = true; // indicate that we restarted.
          } catch ( IOException ioe ) {
            logError( BaseMessages.getString( PKG, "JoinRows.Log.UnableToCloseInputStream" )
              + data.file[ filenr ] + "] : " + ioe.toString() );
            setErrors( 1 );
            stopAll();
            return null;
          }
        }
      } else {
        if ( data.size[ filenr ] == 0 ) {
          if ( log.isBasic() ) {
            logBasic( BaseMessages.getString( PKG, "JoinRows.Log.NoRowsComingFromTransform" )
              + data.rs[ filenr ].getOriginTransformName() + "]" );
          }
          return null;
        }
        rowData = data.cache[ filenr ].get( data.position[ data.filenr ] );

        // Don't forget to clone the data to protect it against data alteration downstream.
        //
        rowData = data.fileRowMeta[ filenr ].cloneRow( rowData );

        data.position[ filenr ]++;

        // If the file is at the end, close it.
        // The file will then be re-opened if needed later on.
        if ( data.position[ filenr ] >= data.size[ filenr ] ) {
          data.position[ filenr ] = 0;
          data.restart[ filenr ] = true; // indicate that we restarted.
        }
      }
    }

    return rowData;
  }

  public boolean processRow() throws HopException {

    if ( first ) {
      first = false;
      initialize();
    }

    if ( data.caching ) {
      if ( !cacheInputRow() ) {
        return false;
      }
    } else {
      if ( !outputRow() ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Write to the output!
   */
  private boolean outputRow() throws HopException {

    // Read one row and store it in joinrow[]
    //
    data.joinrow[ data.filenr ] = getRowData( data.filenr );
    if ( data.joinrow[ data.filenr ] == null ) {
      // 100 x 0 = 0 : don't output when one of the input streams has no rows.
      // If this is filenr #0, it's fine too!
      //
      // Before we exit we need to make sure the 100 rows in the other streams are consumed though...
      //
      while ( getRow() != null ) {
        // Consume
        if ( isStopped() ) {
          break;
        }
      }

      setOutputDone();
      return false;
    }

    //
    // OK, are we at the last file yet?
    // If so, we can output one row in the cartesian product.
    // Otherwise, go to the next file to get an extra row.
    //
    if ( data.filenr >= data.file.length - 1 ) {
      if ( data.outputRowMeta == null ) {
        data.outputRowMeta = createOutputRowMeta( data.fileRowMeta );
      }

      // Stich the output row together
      Object[] sum = new Object[ data.outputRowMeta.size() ];
      int sumIndex = 0;
      for ( int f = 0; f <= data.filenr; f++ ) {
        for ( int c = 0; c < data.fileRowMeta[ f ].size(); c++ ) {
          sum[ sumIndex ] = data.joinrow[ f ][ c ];
          sumIndex++;
        }
      }

      if ( meta.getCondition() != null && !meta.getCondition().isEmpty() ) {
        // Test the specified condition...
        if ( meta.getCondition().evaluate( data.outputRowMeta, sum ) ) {
          putRow( data.outputRowMeta, sum );
        }
      } else {
        // Put it out where it belongs!
        putRow( data.outputRowMeta, sum );
      }

      // Did we reach the last position in the last file?
      // This means that position[] is at 0!
      // Possible we have to do this multiple times.
      //
      while ( data.restart[ data.filenr ] ) {
        // Get row from the previous file
        data.filenr--;
      }
    } else {
      data.filenr++;
    }
    return true;
  }

  private boolean cacheInputRow() throws HopException {
    // /////////////////////////////
    // Read from input channels //
    // /////////////////////////////

    if ( data.filenr >= data.file.length ) {
      // Switch the mode to reading back from the data cache
      data.caching = false;

      // Start back at filenr = 0
      data.filenr = 0;

      return true;
    }

    // We need to open a new outputstream
    if ( data.dataOutputStream[ data.filenr ] == null ) {
      try {
        // Open the temp file
        data.fileOutputStream[ data.filenr ] = new FileOutputStream( data.file[ data.filenr ] );

        // Open the data output stream...
        data.dataOutputStream[ data.filenr ] = new DataOutputStream( data.fileOutputStream[ data.filenr ] );
      } catch ( FileNotFoundException fnfe ) {
        logError( BaseMessages.getString( PKG, "JoinRows.Log.UnableToOpenOutputstream" )
          + data.file[ data.filenr ].toString() + "] : " + fnfe.toString() );
        stopAll();
        setErrors( 1 );
        return false;
      }
    }

    // Read a line from the appropriate rowset...
    IRowSet rowSet = data.rs[ data.filenr ];
    Object[] rowData = getRowFrom( rowSet );
    if ( rowData != null ) {
      // We read a row from one of the input streams...

      if ( data.fileRowMeta[ data.filenr ] == null ) {
        // The first row is used as meta-data, clone it for safety
        data.fileRowMeta[ data.filenr ] = rowSet.getRowMeta().clone();
      }

      data.fileRowMeta[ data.filenr ].writeData( data.dataOutputStream[ data.filenr ], rowData );
      data.size[ data.filenr ]++;

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "JoinRows.Log.ReadRowFromStreamN", data.filenr,
          data.fileRowMeta[ data.filenr ].getString( rowData ) ) );
      }

      //
      // Perhaps we want to cache this data??
      //
      if ( data.size[ data.filenr ] <= meta.getCacheSize() ) {
        if ( data.cache[ data.filenr ] == null ) {
          data.cache[ data.filenr ] = new ArrayList<>();
        }

        // Add this row to the cache!
        data.cache[ data.filenr ].add( rowData );
      } else {
        // we can't cope with this many rows: reset the cache...
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JoinRows.Log.RowsFound", meta.getCacheSize() + "", data.rs[ data.filenr ].getOriginTransformName() ) );
        }
        data.cache[ data.filenr ] = null;
      }

    } else {
      // No more rows found on rowset!!

      // Close outputstream.
      try {
        data.dataOutputStream[ data.filenr ].close();
        data.fileOutputStream[ data.filenr ].close();
        data.dataOutputStream[ data.filenr ] = null;
        data.fileOutputStream[ data.filenr ] = null;
      } catch ( IOException ioe ) {
        logError( BaseMessages.getString( PKG, "JoinRows.Log.ErrorInClosingOutputStream" )
          + data.filenr + " : [" + data.file[ data.filenr ].toString() + "] : " + ioe.toString() );
      }

      // Advance to the next file/input-stream...
      data.filenr++;
    }

    return true;
  }

  private IRowMeta createOutputRowMeta( IRowMeta[] fileRowMeta ) {
    IRowMeta outputRowMeta = new RowMeta();
    for ( int i = 0; i < data.fileRowMeta.length; i++ ) {
      outputRowMeta.mergeRowMeta( data.fileRowMeta[ i ], meta.getName() );
    }
    return outputRowMeta;
  }

  public void dispose() {

    // Remove the temporary files...
    if ( data.file != null ) {
      for ( int i = 1; i < data.file.length; i++ ) {
        if ( data.file[ i ] != null ) {
          data.file[ i ].delete();
        }
      }
    }

    super.dispose();
  }

  @Override
  public void batchComplete() throws HopException {
    IRowSet rowSet = getFirstInputRowSet();
    int repeats = 0;
    for ( int i = 0; i < data.cache.length; i++ ) {
      if ( repeats == 0 ) {
        repeats = 1;
      }
      if ( data.cache[ i ] != null ) {
        repeats *= data.cache[ i ].size();
      }
    }
    while ( rowSet.size() > 0 && !isStopped() ) {
     init();
    }
    // The last row needs to be written too to the account of the number of input rows.
    //
    for ( int i = 0; i < repeats; i++ ) {
     init();
    }
  }
}
