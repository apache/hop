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

package org.apache.hop.beam.core.coder;

import junit.framework.TestCase;
import org.apache.hop.beam.core.HopRow;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

public class HopRowCoderTest extends TestCase {

  ByteArrayOutputStream outputStream;
  private HopRowCoder hopRowCoder;

  @Override protected void setUp() throws Exception {

    outputStream= new ByteArrayOutputStream( 1000000 );
    hopRowCoder = new HopRowCoder();
  }

  @Test
  public void testEncode() throws IOException {

    HopRow row1 = new HopRow(new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) } );

    hopRowCoder.encode( row1, outputStream );
    outputStream.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream( bytes );
    HopRow row1d = hopRowCoder.decode( inputStream );

    assertEquals( row1, row1d );
  }


  @Test
  public void decode() {
  }
}