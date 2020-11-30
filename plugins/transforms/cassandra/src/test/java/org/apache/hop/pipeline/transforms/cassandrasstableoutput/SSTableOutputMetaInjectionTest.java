/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.junit.Before;
import org.junit.Test;

public class SSTableOutputMetaInjectionTest extends BaseMetadataInjectionTest<SSTableOutputMeta> {

  @Before
  public void setup() throws Exception {
    setup( new SSTableOutputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "YAML_FILE_PATH", new IStringGetter() {
      public String get() {
        return meta.getYamlPath();
      }
    } );
    check( "DIRECTORY", new IStringGetter() {
      public String get() {
        return meta.getDirectory();
      }
    } );
    check( "CASSANDRA_KEYSPACE", new IStringGetter() {
      public String get() {
        return meta.getCassandraKeyspace();
      }
    } );
    check( "TABLE", new IStringGetter() {
      public String get() {
        return meta.getTableName();
      }
    } );
    check( "KEY_FIELD", new IStringGetter() {
      public String get() {
        return meta.getKeyField();
      }
    } );
    check( "BUFFER_SIZE", new IStringGetter() {
      public String get() {
        return meta.getBufferSize();
      }
    } );
  }

}
