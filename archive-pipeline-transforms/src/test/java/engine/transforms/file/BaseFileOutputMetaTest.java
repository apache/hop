/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.file;

import org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutputMeta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class BaseFileOutputMetaTest {

  @Mock BaseFileOutputMeta meta;

  @Before
  public void setup() throws Exception {
    meta = Mockito.spy( new TextFileOutputMeta() );
  }

  @Test
  public void testGetFiles() {
    String[] filePaths;

    filePaths = meta.getFiles( "foo", "txt", false );
    assertNotNull( filePaths );
    assertEquals( 1, filePaths.length );
    assertEquals( "foo.txt", filePaths[ 0 ] );
    filePaths = meta.getFiles( "foo", "txt", true );
    assertNotNull( filePaths );
    assertEquals( 1, filePaths.length );
    assertEquals( "foo.txt", filePaths[ 0 ] );

    when( meta.isTransformNrInFilename() ).thenReturn( true );

    filePaths = meta.getFiles( "foo", "txt", false );
    assertNotNull( filePaths );
    assertEquals( 1, filePaths.length );
    assertEquals( "foo_<transform>.txt", filePaths[ 0 ] );
    filePaths = meta.getFiles( "foo", "txt", true );
    assertNotNull( filePaths );
    assertEquals( 4, filePaths.length );
    assertEquals( "foo_0.txt", filePaths[ 0 ] );
    assertEquals( "foo_1.txt", filePaths[ 1 ] );
    assertEquals( "foo_2.txt", filePaths[ 2 ] );
    assertEquals( "...", filePaths[ 3 ] );

    when( meta.isPartNrInFilename() ).thenReturn( true );

    filePaths = meta.getFiles( "foo", "txt", false );
    assertNotNull( filePaths );
    assertEquals( 1, filePaths.length );
    assertEquals( "foo_<transform>_<partition>.txt", filePaths[ 0 ] );

    when( meta.getSplitEvery() ).thenReturn( 1 );

    filePaths = meta.getFiles( "foo", "txt", false );
    assertNotNull( filePaths );
    assertEquals( 1, filePaths.length );
    // assertEquals( "foo_<transform>_<partition>_<split>.txt", filePaths[ 0 ] );

  }

}
