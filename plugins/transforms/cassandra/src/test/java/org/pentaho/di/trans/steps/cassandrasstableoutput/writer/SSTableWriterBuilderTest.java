/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.di.trans.steps.cassandrasstableoutput.writer;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.junit.Test;
import org.pentaho.di.core.row.RowMetaInterface;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SSTableWriterBuilderTest extends SSTableWriterBuilder {

  public static final String KEY_FIELD = "some_key";
  public static final int BUFFER_SIZE = 10;
  public static final String TABLE = "some_table";
  public static final String DIR = "some_dir";
  public static final String CONF_PATH = "some_conf_path";
  public static final String KEYSPACE = "some_keyspace";
  public static final String PARTIONER_CLASS_NAME = "PartionerClassName";
  public static final RowMetaInterface ROW_META = mock( RowMetaInterface.class );

  class CQL3SSTableWriterStub extends CQL3SSTableWriter {
    public CQL3SSTableWriterStub() {
      assertEquals( "file:" + CONF_PATH, System.getProperty( "cassandra.config" ) );
    }

    @Override public void setPrimaryKey( String keyField ) {
      assertEquals( KEY_FIELD, keyField );
    }

    @Override public void setDirectory( String directory ) {
      assertEquals( DIR, directory );
    }

    @Override public void setKeyspace( String keyspace ) {
      assertEquals( KEYSPACE, keyspace );
    }

    @Override public void setTable( String table ) {
      assertEquals( TABLE, table );
    }

    @Override public void setBufferSize( int bufferSize ) {
      assertEquals( BUFFER_SIZE, bufferSize );
    }

    @Override public void setRowMeta( RowMetaInterface rowMeta ) {
      assertEquals( ROW_META, rowMeta );
    }
  }

  @Override String getPartitionerClass() throws ConfigurationException {
    return PARTIONER_CLASS_NAME;
  }

  @Override CQL3SSTableWriter getCql3SSTableWriter() {
    return new CQL3SSTableWriterStub();
  }

  @Test
  public void testBuild2() throws Exception {
    SSTableWriterBuilder ssTableWriterBuilder = new SSTableWriterBuilderTest();
    ssTableWriterBuilder = ssTableWriterBuilder.withConfig( CONF_PATH ).withBufferSize( BUFFER_SIZE )
      .withTable( TABLE ).withCqlVersion( 2 ).withDirectory( DIR )
      .withPrimaryKey( KEY_FIELD ).withKeyspace( KEYSPACE ).withRowMeta( ROW_META );
    ssTableWriterBuilder.build();
  }

  @Test
  public void testBuild3() throws Exception {
    SSTableWriterBuilder ssTableWriterBuilder = new SSTableWriterBuilderTest();

    ssTableWriterBuilder = ssTableWriterBuilder.withConfig( CONF_PATH ).withBufferSize( BUFFER_SIZE )
      .withTable( TABLE ).withCqlVersion( 3 ).withDirectory( DIR )
      .withPrimaryKey( KEY_FIELD ).withKeyspace( KEYSPACE ).withRowMeta( ROW_META );
    ssTableWriterBuilder.build();
  }
}
