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

package org.apache.hop.pipeline.transforms.excelinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.junit.Before;
import org.junit.Ignore;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Base class for all tests for BaseFileInput transforms.
 */
@Ignore( "No tests in abstract base class" )
public abstract class BaseParsingTest<Meta extends ITransformMeta, Data extends ITransformData, Transform extends BaseTransform> {

  protected ILogChannel log = new LogChannel( "junit" );
  protected FileSystemManager fs;
  protected String inPrefix;
  protected Meta meta;
  protected Data data;
  protected Transform transform;
  protected TransformMeta transformMeta;
  protected PipelineMeta pipelineMeta;
  protected Pipeline pipeline;

  protected List<Object[]> rows = new ArrayList<>();
  protected int errorsCount;

  /**
   * Initialize transform info. Method is final against redefine in descendants.
   */
  @Before
  public final void beforeCommon() throws Exception {
    HopEnvironment.init();
    PluginRegistry.addPluginType( CompressionPluginType.getInstance() );
    PluginRegistry.init( false );

    transformMeta = new TransformMeta();
    transformMeta.setName( "test" );

    pipeline = new LocalPipelineEngine();
    pipeline.setLogChannel( log );
    pipeline.setRunning( true );
    pipelineMeta = new PipelineMeta() {
      @Override
      public TransformMeta findTransform( String name ) {
        return transformMeta;
      }
    };

    fs = VFS.getManager();
    inPrefix = '/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/files/";
  }

  /**
   * Resolve file from test directory.
   */
  protected FileObject getFile( String filename ) throws Exception {
    URL res = this.getClass().getResource( inPrefix + filename );
    assertNotNull( "There is no file", res );
    FileObject file = fs.resolveFile( res.toExternalForm() );
    assertNotNull( "There is no file", file );
    return file;
  }

  /**
   * Declare fields for test.
   */
  protected abstract void setFields( BaseFileField... fields ) throws Exception;

  /**
   * Process all rows.
   */
  protected void process() throws Exception {
    //CHECKSTYLE IGNORE EmptyBlock FOR NEXT 3 LINES
    while ( transform.processRow() ) {
      // nothing here - just make sure the rows process
    }
  }

  /**
   * Check result of parsing.
   *
   * @param expected array of rows of fields, i.e. { {"field 1 value in row 1","field 2 value in row 1"}, {
   *                 "field 1 value in row 2","field 2 value in row 2"} }
   */
  protected void check( Object[][] expected ) throws Exception {
    checkErrors();
    checkRowCount( expected );
  }

  /**
   * Check result no has errors.
   */
  protected void checkErrors() throws Exception {
    assertEquals( "There are errors", 0, errorsCount );
    assertEquals( "There are transform errors", 0, transform.getErrors() );
  }

  /**
   * Check result of parsing.
   *
   * @param expected array of rows of fields, i.e. { {"field 1 value in row 1","field 2 value in row 1"}, {
   *                 "field 1 value in row 2","field 2 value in row 2"} }
   */
  protected void checkRowCount( Object[][] expected ) throws Exception {
    assertEquals( "Wrong rows count", expected.length, rows.size() );
    checkContent( expected );
  }

  /**
   * Check content of parsing.
   *
   * @param expected array of rows of fields, i.e. { {"field 1 value in row 1","field 2 value in row 1"}, {
   *                 "field 1 value in row 2","field 2 value in row 2"} }
   */
  protected void checkContent( Object[][] expected ) throws Exception {
    for ( int i = 0; i < expected.length; i++ ) {
      assertArrayEquals( "Wrong row: " + Arrays.asList( rows.get( i ) ), expected[ i ], rows.get( i ) );
    }
  }

  /**
   * Listener for parsing result.
   */
  protected IRowListener rowListener = new IRowListener() {
    @Override
    public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      rows.add( Arrays.copyOf( row, rowMeta.size() ) );
    }

    @Override
    public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      System.out.println();
    }

    @Override
    public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      errorsCount++;
    }
  };
}
