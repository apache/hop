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

package org.apache.hop.pipeline.transforms.update;

import junit.framework.TestCase;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.utils.RowMetaUtils;
import org.junit.Test;

public class UpdateSqlTest extends TestCase {

  @Test
  public void testRowsTransform() {

    String[] keyLookup = new String[] { "Name" };
    String[] keyStream = new String[] { "FirstName" };
    String[] updateLookup = new String[] { "SecondName", "PostAddress", "ZIP" };
    String[] updateStream = new String[] { "SurName", "Address", "ZIP" };

    IRowMeta prev = new RowMeta();
    prev.addValueMeta( new ValueMetaString( keyStream[ 0 ] ) );
    prev.addValueMeta( new ValueMetaString( updateStream[ 0 ] ) );
    prev.addValueMeta( new ValueMetaString( updateStream[ 1 ] ) );
    prev.addValueMeta( new ValueMetaString( updateStream[ 2 ] ) );

    try {
      IRowMeta result =
        RowMetaUtils.getRowMetaForUpdate( prev, keyLookup, keyStream, updateLookup, updateStream );

      IValueMeta vmi = result.getValueMeta( 0 );
      assertEquals( vmi.getName(), keyLookup[ 0 ] );
      assertEquals( prev.getValueMeta( 0 ).getName(), keyStream[ 0 ] );

      assertEquals( result.getValueMeta( 1 ).getName(), updateLookup[ 0 ] );
      assertEquals( prev.getValueMeta( 1 ).getName(), updateStream[ 0 ] );

      assertEquals( result.getValueMeta( 2 ).getName(), updateLookup[ 1 ] );
      assertEquals( prev.getValueMeta( 2 ).getName(), updateStream[ 1 ] );

      assertEquals( result.getValueMeta( 3 ).getName(), updateStream[ 2 ] );
      assertEquals( prev.getValueMeta( 3 ).getName(), updateStream[ 2 ] );
    } catch ( Exception ex ) {
      ex.printStackTrace();
      fail();
    }
  }

}
