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

package org.apache.hop.concurrency;

import org.apache.commons.collections4.ListUtils;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test consists of two similar cases. There are three type of actors: getters, searchers and updaters. They work
 * simultaneously within their own threads. Getters invoke {@linkplain WorkflowTracker#getWorkflowTracker(int)} with a random
 * index, searchers call {@linkplain WorkflowTracker#findWorkflowTracker(ActionMeta)}, updaters add new children
 * <tt>updatersCycles</tt> times. The difference between two cases is the second has a small limit of stored children,
 * so the parent WorkflowTracker will be forced to remove some of its elements.
 *
 * @author Andrey Khayrutdinov
 */
@RunWith( Parameterized.class )
public class WorkflowTrackerConcurrencyTest {

  private static final int gettersAmount = 10;

  private static final int searchersAmount = 20;

  private static final int updatersAmount = 5;
  private static final int updatersCycles = 10;

  private static final int jobsLimit = 20;

  @SuppressWarnings( "ConstantConditions" )
  @BeforeClass
  public static void setUp() {
    // a guarding check for tests' parameters
    int jobsToBeAdded = updatersAmount * updatersCycles;
    assertTrue( "The limit of stored workflows must be less than the amount of children to be added",
      jobsLimit < jobsToBeAdded );
  }


  @Parameterized.Parameters
  public static List<Object[]> getData() {
    return Arrays.asList(
      new Object[] { new WorkflowTracker( mockWorkflowMeta( "parent" ) ) },
      new Object[] { new WorkflowTracker( mockWorkflowMeta( "parent" ), jobsLimit ) }
    );
  }

  private static WorkflowMeta mockWorkflowMeta( String name ) {
    WorkflowMeta meta = mock( WorkflowMeta.class );
    when( meta.getName() ).thenReturn( name );
    return meta;
  }


  private final WorkflowTracker tracker;

  public WorkflowTrackerConcurrencyTest( WorkflowTracker tracker ) {
    this.tracker = tracker;
  }

  @Test
  public void readAndUpdateTrackerConcurrently() throws Exception {
    final AtomicBoolean condition = new AtomicBoolean( true );

    List<Getter> getters = new ArrayList<>( gettersAmount );
    for ( int i = 0; i < gettersAmount; i++ ) {
      getters.add( new Getter( condition, tracker ) );
    }

    List<Searcher> searchers = new ArrayList<>( searchersAmount );
    for ( int i = 0; i < searchersAmount; i++ ) {
      int lookingFor = updatersAmount * updatersCycles / 2 + i;
      assertTrue( "We are looking for reachable index", lookingFor < updatersAmount * updatersCycles );
      searchers.add( new Searcher( condition, tracker, mockActionMeta( "workflow-action-" + lookingFor ) ) );
    }

    final AtomicInteger generator = new AtomicInteger( 0 );
    List<Updater> updaters = new ArrayList<>( updatersAmount );
    for ( int i = 0; i < updatersAmount; i++ ) {
      updaters.add( new Updater( tracker, updatersCycles, generator, "workflow-action-%d" ) );
    }

    //noinspection unchecked
    ConcurrencyTestRunner.runAndCheckNoExceptionRaised( updaters, ListUtils.union( getters, searchers ), condition );
    assertEquals( updatersAmount * updatersCycles, generator.get() );
  }

  static ActionMeta mockActionMeta( String name ) {
    ActionMeta copy = mock( ActionMeta.class );
    when( copy.getName() ).thenReturn( name );
    return copy;
  }


  private static class Getter extends StopOnErrorCallable<Object> {
    private final WorkflowTracker tracker;
    private final Random random;

    public Getter( AtomicBoolean condition, WorkflowTracker tracker ) {
      super( condition );
      this.tracker = tracker;
      this.random = new Random();
    }

    @Override
    public Object doCall() throws Exception {
      while ( condition.get() ) {
        int amount = tracker.nrWorkflowTrackers();
        if ( amount == 0 ) {
          continue;
        }
        int i = random.nextInt( amount );
        WorkflowTracker t = tracker.getWorkflowTracker( i );
        if ( t == null ) {
          throw new IllegalStateException(
            String.format( "Returned tracker must not be null. Index = %d, trackers' amount = %d", i, amount ) );
        }
      }
      return null;
    }
  }


  private static class Searcher extends StopOnErrorCallable<Object> {
    private final WorkflowTracker tracker;
    private final ActionMeta copy;

    public Searcher( AtomicBoolean condition, WorkflowTracker tracker, ActionMeta copy ) {
      super( condition );
      this.tracker = tracker;
      this.copy = copy;
    }

    @Override Object doCall() throws Exception {
      while ( condition.get() ) {
        // can be null, it is OK here
        tracker.findWorkflowTracker( copy );
      }
      return null;
    }
  }


  private static class Updater implements Callable<Exception> {
    private final WorkflowTracker tracker;
    private final int cycles;
    private final AtomicInteger idGenerator;
    private final String resultNameTemplate;

    public Updater( WorkflowTracker tracker, int cycles, AtomicInteger idGenerator, String resultNameTemplate ) {
      this.tracker = tracker;
      this.cycles = cycles;
      this.idGenerator = idGenerator;
      this.resultNameTemplate = resultNameTemplate;
    }

    @Override
    public Exception call() throws Exception {
      Exception exception = null;
      try {
        for ( int i = 0; i < cycles; i++ ) {
          int id = idGenerator.getAndIncrement();
          ActionResult result = new ActionResult();
          result.setActionName( String.format( resultNameTemplate, id ) );
          WorkflowTracker child = new WorkflowTracker( mockWorkflowMeta( "child-" + id ), result );
          tracker.addWorkflowTracker( child );
        }
      } catch ( Exception e ) {
        exception = e;
      }
      return exception;
    }
  }
}
