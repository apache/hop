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

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Khayrutdinov
 */
public class RowMetaConcurrencyTest {

  private static final int cycles = 50;

  @Test
  public void fiveAddersAgainstTenReaders() throws Exception {
    final int addersAmount = 5;
    final int readersAmount = 10;

    final AtomicBoolean condition = new AtomicBoolean( true );
    final RowMeta rowMeta = new RowMeta();

    List<Adder> adders = new ArrayList<>( addersAmount );
    for ( int i = 0; i < addersAmount; i++ ) {
      adders.add( new Adder( condition, rowMeta, cycles, "adder" + i ) );
    }

    List<Getter> getters = new ArrayList<>( readersAmount );
    for ( int i = 0; i < readersAmount; i++ ) {
      getters.add( new Getter( condition, rowMeta ) );
    }

    ConcurrencyTestRunner<List<IValueMeta>, ?> runner =
      new ConcurrencyTestRunner<>( adders, getters, condition );
    runner.runConcurrentTest();

    runner.checkNoExceptionRaised();

    Set<IValueMeta> results = new HashSet<>( cycles * addersAmount );
    for ( List<IValueMeta> list : runner.getMonitoredTasksResults() ) {
      results.addAll( list );
    }
    List<IValueMeta> metas = rowMeta.getValueMetaList();

    assertEquals( cycles * addersAmount, metas.size() );
    assertEquals( cycles * addersAmount, results.size() );
    for ( IValueMeta meta : metas ) {
      assertTrue( meta.getName(), results.remove( meta ) );
    }
    assertTrue( results.isEmpty() );
  }


  @Test
  public void fiveShufflersAgainstTenSearchers() throws Exception {
    final int elementsAmount = 100;
    final int shufflersAmount = 5;
    final int searchersAmount = 10;

    final RowMeta rowMeta = new RowMeta();
    for ( int i = 0; i < elementsAmount; i++ ) {
      rowMeta.addValueMeta( new ValueMetaString( "meta_" + i ) );
    }

    final AtomicBoolean condition = new AtomicBoolean( true );

    List<Shuffler> shufflers = new ArrayList<>( shufflersAmount );
    for ( int i = 0; i < shufflersAmount; i++ ) {
      shufflers.add( new Shuffler( condition, rowMeta, cycles ) );
    }

    List<Searcher> searchers = new ArrayList<>( searchersAmount );
    for ( int i = 0; i < searchersAmount; i++ ) {
      String name = "meta_" + ( new Random().nextInt( elementsAmount ) );
      assertTrue( rowMeta.indexOfValue( name ) >= 0 );
      searchers.add( new Searcher( condition, rowMeta, name ) );
    }

    ConcurrencyTestRunner.runAndCheckNoExceptionRaised( shufflers, searchers, condition );
  }


  @SuppressWarnings( "unchecked" )
  @Test
  public void addRemoveSearch() throws Exception {
    final int addersAmount = 5;
    final int removeAmount = 10;
    final int searchersAmount = 10;

    final RowMeta rowMeta = new RowMeta();
    List<String> toRemove = new ArrayList<>( removeAmount );
    for ( int i = 0; i < removeAmount; i++ ) {
      String name = "toBeRemoved_" + i;
      toRemove.add( name );
      rowMeta.addValueMeta( new ValueMetaString( name ) );
    }

    final AtomicBoolean condition = new AtomicBoolean( true );

    List<Searcher> searchers = new ArrayList<>( searchersAmount );
    for ( int i = 0; i < searchersAmount; i++ ) {
      String name = "kept_" + i;
      rowMeta.addValueMeta( new ValueMetaString( name ) );
      searchers.add( new Searcher( condition, rowMeta, name ) );
    }


    List<Remover> removers = Collections.singletonList( new Remover( condition, rowMeta, toRemove ) );

    List<Adder> adders = new ArrayList<>( addersAmount );
    for ( int i = 0; i < addersAmount; i++ ) {
      adders.add( new Adder( condition, rowMeta, cycles, "adder" + i ) );
    }

    List<Callable<?>> monitored = new ArrayList<>();
    monitored.addAll( adders );
    monitored.addAll( removers );


    ConcurrencyTestRunner<?, ?> runner =
      new ConcurrencyTestRunner<>( monitored, searchers, condition );
    runner.runConcurrentTest();

    runner.checkNoExceptionRaised();

    Map<? extends Callable<?>, ? extends ExecutionResult<?>> results = runner.getMonitoredResults();

    // removers should remove all elements
    for ( Remover remover : removers ) {
      ExecutionResult<List<String>> result = (ExecutionResult<List<String>>) results.get( remover );
      assertTrue( result.getResult().isEmpty() );
      for ( String name : remover.getToRemove() ) {
        assertEquals( name, -1, rowMeta.indexOfValue( name ) );
      }
    }
    // adders should add all elements
    Set<IValueMeta> metas = new HashSet<>( rowMeta.getValueMetaList() );
    for ( Adder adder : adders ) {
      ExecutionResult<List<IValueMeta>> result =
        (ExecutionResult<List<IValueMeta>>) results.get( adder );
      for ( IValueMeta meta : result.getResult() ) {
        assertTrue( meta.getName(), metas.remove( meta ) );
      }
    }
    assertEquals( searchersAmount, metas.size() );
  }


  private static class Getter extends StopOnErrorCallable<Object> {

    private final RowMeta rowMeta;

    public Getter( AtomicBoolean condition, RowMeta rowMeta ) {
      super( condition );
      this.rowMeta = rowMeta;
    }

    @Override
    Object doCall() throws Exception {
      Random random = new Random();
      while ( condition.get() ) {
        int acc = 0;
        for ( IValueMeta meta : rowMeta.getValueMetaList() ) {
          // fake cycle to from eliminating this snippet by JIT
          acc += meta.getType() / 10;
        }
        Thread.sleep( random.nextInt( Math.max( 100, acc ) ) );
      }
      return null;
    }
  }

  private static class Adder extends StopOnErrorCallable<List<IValueMeta>> {
    private final RowMeta rowMeta;
    private final int cycles;
    private final String nameSeed;

    public Adder( AtomicBoolean condition, RowMeta rowMeta, int cycles, String nameSeed ) {
      super( condition );
      this.rowMeta = rowMeta;
      this.cycles = cycles;
      this.nameSeed = nameSeed;
    }

    @Override
    List<IValueMeta> doCall() throws Exception {
      Random random = new Random();
      List<IValueMeta> result = new ArrayList<>( cycles );
      for ( int i = 0; ( i < cycles ) && condition.get(); i++ ) {
        IValueMeta added = new ValueMetaString( nameSeed + '_' + i );
        rowMeta.addValueMeta( added );
        result.add( added );
        Thread.sleep( random.nextInt( 100 ) );
      }
      return result;
    }
  }

  private static class Searcher extends StopOnErrorCallable<Object> {
    private final RowMeta rowMeta;
    private final String name;

    public Searcher( AtomicBoolean condition, RowMeta rowMeta, String name ) {
      super( condition );
      this.rowMeta = rowMeta;
      this.name = name;
    }

    @Override
    Object doCall() throws Exception {
      Random random = new Random();
      while ( condition.get() ) {
        int index = rowMeta.indexOfValue( name );
        if ( index < 0 ) {
          throw new IllegalStateException( name + " was not found among " + rowMeta.getValueMetaList() );
        }
        Thread.sleep( random.nextInt( 100 ) );
      }
      return null;
    }
  }

  private static class Shuffler extends StopOnErrorCallable<Object> {
    private final RowMeta rowMeta;
    private final int cycles;

    public Shuffler( AtomicBoolean condition, RowMeta rowMeta, int cycles ) {
      super( condition );
      this.rowMeta = rowMeta;
      this.cycles = cycles;
    }

    @Override
    Object doCall() throws Exception {
      Random random = new Random();
      for ( int i = 0; ( i < cycles ) && condition.get(); i++ ) {
        List<IValueMeta> list = new ArrayList<>( rowMeta.getValueMetaList() );
        Collections.shuffle( list );
        rowMeta.setValueMetaList( list );
        Thread.sleep( random.nextInt( 100 ) );
      }
      return null;
    }
  }

  private static class Remover extends StopOnErrorCallable<List<String>> {
    private final RowMeta rowMeta;
    private final List<String> toRemove;

    public Remover( AtomicBoolean condition, RowMeta rowMeta, List<String> toRemove ) {
      super( condition );
      this.rowMeta = rowMeta;
      this.toRemove = toRemove;
    }

    @Override
    List<String> doCall() throws Exception {
      Random random = new Random();
      List<String> result = new LinkedList<>( toRemove );
      for ( Iterator<String> it = result.iterator(); it.hasNext() && condition.get(); ) {
        String name = it.next();
        rowMeta.removeValueMeta( name );
        it.remove();
        Thread.sleep( random.nextInt( 100 ) );
      }
      return result;
    }

    public List<String> getToRemove() {
      return toRemove;
    }
  }
}
