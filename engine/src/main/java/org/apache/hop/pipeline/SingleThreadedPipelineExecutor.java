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

package org.apache.hop.pipeline;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.List;

public class SingleThreadedPipelineExecutor {

  private final List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> transforms;
  private Pipeline pipeline;
  private boolean[] done;
  private int nrDone;
  private List<List<IStream>> transformInfoStreams;
  private List<List<IRowSet>> transformInfoRowSets;
  private ILogChannel log;

  public SingleThreadedPipelineExecutor( final Pipeline pipeline ) {
    this.pipeline = pipeline;
    this.log = pipeline.getLogChannel();

    transforms = pipeline.getTransforms();

    sortTransforms();

    done = new boolean[ transforms.size() ];
    nrDone = 0;

    transformInfoStreams = new ArrayList<>();
    transformInfoRowSets = new ArrayList<>();
    for ( TransformMetaDataCombi combi : transforms ) {
      List<IStream> infoStreams = combi.transformMeta.getTransform().getTransformIOMeta().getInfoStreams();
      transformInfoStreams.add( infoStreams );
      List<IRowSet> infoRowSets = new ArrayList<>();
      for ( IStream infoStream : infoStreams ) {
        IRowSet infoRowSet = pipeline.findRowSet( infoStream.getTransformName(), 0, combi.transformName, 0 );
        if ( infoRowSet != null ) {
          infoRowSets.add( infoRowSet );
        }
      }
      transformInfoRowSets.add( infoRowSets );
    }

  }

  /**
   * Sort the transforms from start to finish...
   */
  private void sortTransforms() {

    // The bubble sort algorithm in contrast to the QuickSort or MergeSort
    // algorithms
    // does indeed cover all possibilities.
    // Sorting larger pipelines with hundreds of transforms might be too slow
    // though.
    // We should consider caching PipelineMeta.findPrevious() results in that case.
    //
    pipeline.getPipelineMeta().clearCaches();

    //
    // Cocktail sort (bi-directional bubble sort)
    //
    // Original sort was taking 3ms for 30 transforms
    // cocktail sort takes about 8ms for the same 30, but it works :)

    // set these to true if you are working on this algorithm and don't like
    // flying blind.
    //
    boolean testing = true; // log sort details

    int transformsMinSize = 0;
    int transformsSize = transforms.size();

    // Noticed a problem with an immediate shrinking iteration window
    // trapping rows that need to be sorted.
    // This threshold buys us some time to get the sorting close before
    // starting to decrease the window size.
    //
    // TODO: this could become much smarter by tracking row movement
    // and reacting to that each outer iteration verses
    // using a threshold.
    //
    // After this many iterations enable trimming inner iteration
    // window on no change being detected.
    //
    int windowShrinkThreshold = (int) Math.round( transformsSize * 0.75 );

    // give ourselves some room to sort big lists. the window threshold should
    // stop us before reaching this anyway.
    //
    int totalIterations = transformsSize * 2;
    int actualIterations = 0;

    boolean isBefore = false;
    boolean forwardChange = false;
    boolean backwardChange = false;

    boolean lastForwardChange = true;
    boolean keepSortingForward = true;

    TransformMetaDataCombi one = null;
    TransformMetaDataCombi two = null;

    StringBuilder tLogString = new StringBuilder(); // this helps group our
    // output so other threads
    // don't get logs in our
    // output.
    tLogString.append( "-------------------------------------------------------" ).append( "\n" );
    tLogString.append( "--SingleThreadedPipelineExecutor.sortTransforms(cocktail)" ).append( "\n" );
    tLogString.append( "--Pipeline: " ).append( pipeline.getName() ).append( "\n" );
    tLogString.append( "-" ).append( "\n" );

    long startTime = System.currentTimeMillis();

    for ( int x = 0; x < totalIterations; x++ ) {

      // Go forward through the list
      //
      if ( keepSortingForward ) {
        for ( int y = transformsMinSize; y < transformsSize - 1; y++ ) {
          one = transforms.get( y );
          two = transforms.get( y + 1 );
          isBefore = pipeline.getPipelineMeta().findPrevious( one.transformMeta, two.transformMeta );
          if ( isBefore ) {
            // two was found to be positioned BEFORE one so we need to
            // switch them...
            //
            transforms.set( y, two );
            transforms.set( y + 1, one );
            forwardChange = true;

          }
        }
      }

      // Go backward through the list
      //
      for ( int z = transformsSize - 1; z > transformsMinSize; z-- ) {
        one = transforms.get( z );
        two = transforms.get( z - 1 );

        isBefore = pipeline.getPipelineMeta().findPrevious( one.transformMeta, two.transformMeta );
        if ( !isBefore ) {
          // two was found NOT to be positioned BEFORE one so we need to
          // switch them...
          //
          transforms.set( z, two );
          transforms.set( z - 1, one );
          backwardChange = true;
        }
      }

      // Shrink transformsSize(max) if there was no forward change
      //
      if ( x > windowShrinkThreshold && !forwardChange ) {

        // should we keep going? check the window size
        //
        transformsSize--;
        if ( transformsSize <= transformsMinSize ) {
          if ( testing ) {
            tLogString.append( String.format( "transformsMinSize:%s  transformsSize:%s", transformsMinSize, transformsSize ) );
            tLogString
              .append( "transformsSize is <= transformsMinSize.. exiting outer sort loop. index:" + x ).append( "\n" );
          }
          break;
        }
      }

      // shrink transformsMinSize(min) if there was no backward change
      //
      if ( x > windowShrinkThreshold && !backwardChange ) {

        // should we keep going? check the window size
        //
        transformsMinSize++;
        if ( transformsMinSize >= transformsSize ) {
          if ( testing ) {
            tLogString.append( String.format( "transformsMinSize:%s  transformsSize:%s", transformsMinSize, transformsSize ) ).append(
              "\n" );
            tLogString
              .append( "transformsMinSize is >= transformsSize.. exiting outer sort loop. index:" + x ).append( "\n" );
          }
          break;
        }
      }

      // End of both forward and backward traversal.
      // Time to see if we should keep going.
      //
      actualIterations++;

      if ( !forwardChange && !backwardChange ) {
        if ( testing ) {
          tLogString.append( String.format( "existing outer loop because no "
              + "change was detected going forward or backward. index:%s  min:%s  max:%s",
            x, transformsMinSize, transformsSize ) ).append( "\n" );
        }
        break;
      }

      //
      // if we are past the first iteration and there has been no change twice,
      // quit doing it!
      //
      if ( keepSortingForward && x > 0 && !lastForwardChange && !forwardChange ) {
        keepSortingForward = false;
      }
      lastForwardChange = forwardChange;
      forwardChange = false;
      backwardChange = false;

    } // finished sorting

    long endTime = System.currentTimeMillis();
    long totalTime = ( endTime - startTime );

    tLogString.append( "-------------------------------------------------------" ).append( "\n" );
    tLogString.append( "Transforms sort time: " + totalTime + "ms" ).append( "\n" );
    tLogString.append( "Total iterations: " + actualIterations ).append( "\n" );
    tLogString.append( "Transform count: " + transforms.size() ).append( "\n" );
    tLogString.append( "Transforms after sort: " ).append( "\n" );
    for ( TransformMetaDataCombi combi : transforms ) {
      tLogString.append( combi.transform.getTransformName() ).append( "\n" );
    }
    tLogString.append( "-------------------------------------------------------" ).append( "\n" );

    if ( log.isDetailed() ) {
      log.logDetailed( tLogString.toString() );
    }
  }

  public boolean init() throws HopException {

    // Initialize all the transforms...
    //
    for ( TransformMetaDataCombi combi : transforms ) {
      boolean ok = combi.transform.init();
      if ( !ok ) {
        return false;
      }
    }
    return true;

  }

  /**
   * Give all transforms in the pipeline the chance to process all rows on input...
   *
   * @return true if more iterations can be performed. False if this is not the case.
   */
  public boolean oneIteration() throws HopException {

    for ( int s = 0; s < transforms.size() && !pipeline.isStopped(); s++ ) {
      if ( !done[ s ] ) {

        TransformMetaDataCombi combi = transforms.get( s );

        // If this transform is waiting for data (text, db, and so on), we simply read all the data
        // This means that it is impractical to use this pipeline type to load large files.
        //
        boolean transformDone = false;
        // For every input row we call the processRow() method of the transform.
        //
        List<IRowSet> infoRowSets = transformInfoRowSets.get( s );

        // Loop over info-rowsets FIRST to make sure we support the "Stream Lookup" transform and so on.
        //
        for ( IRowSet rowSet : infoRowSets ) {
          boolean once = true;
          while ( once || ( rowSet.size() > 0 && !transformDone ) ) {
            once = false;
            transformDone = !combi.transform.processRow();
            if ( combi.transform.getErrors() > 0 ) {
              return false;
            }
          }
        }

        // Do normal processing of input rows...
        //
        List<IRowSet> rowSets = combi.transform.getInputRowSets();

        // If there are no input row sets, we read all rows until finish.
        // This applies to transforms like "Table Input", "Text File Input" and so on.
        // If they do have an input row set, to get filenames or other parameters,
        // we need to handle this in the batchComplete() methods.
        //
        if ( rowSets.size() == 0 ) {
          while ( !transformDone && !pipeline.isStopped() ) {
            transformDone = !combi.transform.processRow();
            if ( combi.transform.getErrors() > 0 ) {
              return false;
            }
          }
        } else {
          // Since we can't be sure that the transform actually reads from the row sets where we measure rows,
          // we simply count the total nr of rows on input. The transforms will find the rows in either row set.
          //
          int nrRows = 0;
          for ( IRowSet rowSet : rowSets ) {
            nrRows += rowSet.size();
          }

          // Now do the number of processRows() calls.
          //
          for ( int i = 0; i < nrRows; i++ ) {
            transformDone = !combi.transform.processRow();
            if ( combi.transform.getErrors() > 0 ) {
              return false;
            }
          }
        }

        // Signal the transform that a batch of rows has passed for this iteration (sort rows and all)
        //
        combi.transform.batchComplete();
        if ( transformDone ) {
          nrDone++;
        }

        done[ s ] = transformDone;
      }
    }

    return nrDone < transforms.size() && !pipeline.isStopped();
  }

  protected int getTotalRows( List<IRowSet> rowSets ) {
    int total = 0;
    for ( IRowSet rowSet : rowSets ) {
      total += rowSet.size();
    }
    return total;
  }

  public long getErrors() {
    return pipeline.getErrors();
  }

  public Result getResult() {
    return pipeline.getResult();
  }

  public boolean isStopped() {
    return pipeline.isStopped();
  }

  public void dispose() throws HopException {

    // Call output done.
    //
    for ( TransformMetaDataCombi combi : pipeline.getTransforms() ) {
      combi.transform.setOutputDone();
    }

    // Finalize all the transforms...
    //
    for ( TransformMetaDataCombi combi : transforms ) {
      combi.transform.dispose();
      combi.transform.markStop();
    }

  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * Clear the error in the pipeline, clear all the rows from all the row sets...
   */
  public void clearError() {
    pipeline.clearError();
  }
}
