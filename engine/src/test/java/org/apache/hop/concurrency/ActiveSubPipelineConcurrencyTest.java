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

package org.apache.hop.concurrency;

import org.apache.hop.pipeline.Pipeline;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In this test we add new elements to shared pipeline concurrently
 * and get added elements from this pipeline concurrently.
 * <p>
 * When working with {@link java.util.HashMap} with default loadFactor this test will fail
 * when HashMap will try to rearrange it's elements (it will happen when number of elements in map will be equal to
 * capacity/loadFactor).
 * <p>
 * Map will be in inconsistent state, because in the same time, when rearrange happens other threads will be adding
 * new elements to map.
 * This will lead to unpredictable result of executing {@link java.util.HashMap#size()} method (as a result there
 * would be an error in {@link Getter#call()} ).
 */
public class ActiveSubPipelineConcurrencyTest {
  private static final int NUMBER_OF_GETTERS = 10;
  private static final int NUMBER_OF_CREATES = 10;
  private static final int NUMBER_OF_CREATE_CYCLES = 20;
  private static final int INITIAL_NUMBER_OF_PIPELINE = 100;


  private static final String PIPELINE_NAME = "pipeline";
  private final Object lock = new Object();

  @Test
  public void getAndCreateConcurrently() throws Exception {
    AtomicBoolean condition = new AtomicBoolean( true );
    Pipeline pipeline = new Pipeline();
    createSubPipeline( pipeline );

    List<Getter> getters = generateGetters( pipeline, condition );
    List<Creator> creators = generateCreators( pipeline, condition );

    ConcurrencyTestRunner.runAndCheckNoExceptionRaised( creators, getters, condition );
  }

  private void createSubPipeline( Pipeline pipeline ) {
    for ( int i = 0; i < INITIAL_NUMBER_OF_PIPELINE; i++ ) {
      pipeline.addActiveSubPipeline( createPipelineName( i ), new Pipeline() );
    }
  }

  private List<Getter> generateGetters( Pipeline pipeline, AtomicBoolean condition ) {
    List<Getter> getters = new ArrayList<>();
    for ( int i = 0; i < NUMBER_OF_GETTERS; i++ ) {
      getters.add( new Getter( pipeline, condition ) );
    }

    return getters;
  }

  private List<Creator> generateCreators( Pipeline pipeline, AtomicBoolean condition ) {
    List<Creator> creators = new ArrayList<Creator>();
    for ( int i = 0; i < NUMBER_OF_CREATES; i++ ) {
      creators.add( new Creator( pipeline, condition ) );
    }

    return creators;
  }


  private class Getter extends StopOnErrorCallable<Object> {
    private final Pipeline pipeline;
    private final Random random;

    Getter( Pipeline pipeline, AtomicBoolean condition ) {
      super( condition );
      this.pipeline = pipeline;
      random = new Random();
    }

    @Override
    Object doCall() throws Exception {
      while ( condition.get() ) {
        final String activeSubPipelineName = createPipelineName( random.nextInt( INITIAL_NUMBER_OF_PIPELINE ) );
        Pipeline subPipeline = pipeline.getActiveSubPipeline( activeSubPipelineName );

        if ( subPipeline == null ) {
          throw new IllegalStateException(
            String.format(
              "Returned pipeline must not be null. Pipeline name = %s",
              activeSubPipelineName ) );
        }
      }

      return null;
    }
  }

  private class Creator extends StopOnErrorCallable<Object> {
    private final Pipeline pipeline;
    private final Random random;

    Creator( Pipeline pipeline, AtomicBoolean condition ) {
      super( condition );
      this.pipeline = pipeline;
      random = new Random();
    }

    @Override
    Object doCall() throws Exception {
      for ( int i = 0; i < NUMBER_OF_CREATE_CYCLES; i++ ) {
        synchronized ( lock ) {
          String pipelineName = createPipelineName( randomInt( INITIAL_NUMBER_OF_PIPELINE, Integer.MAX_VALUE ) );
          pipeline.addActiveSubPipeline( pipelineName, new Pipeline() );
        }
      }
      return null;
    }

    private int randomInt( int min, int max ) {
      return random.nextInt( max - min ) + min;
    }
  }

  private String createPipelineName( int id ) {
    return PIPELINE_NAME + " - " + id;
  }
}
