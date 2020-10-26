/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.pipeline.transforms.orabulkloader;

import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.poi.util.TempFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class OraBulkDataOutputTest {
  private static final String loadMethod = "AUTO_END";
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private OraBulkLoaderMeta oraBulkLoaderMeta;
  private OraBulkDataOutput oraBulkDataOutput;
  private Process sqlldrProcess;
  private IVariables variables;

  @BeforeClass
  public static void setupBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    String recTerm = Const.CR;
    sqlldrProcess = mock( Process.class );
    variables = mock( IVariables.class );
    oraBulkLoaderMeta = mock( OraBulkLoaderMeta.class );
    oraBulkDataOutput = spy( new OraBulkDataOutput( oraBulkLoaderMeta, recTerm ) );

    when( oraBulkLoaderMeta.getLoadMethod() ).thenReturn( loadMethod );
    when( oraBulkLoaderMeta.getEncoding() ).thenReturn( null );
  }

  @Test
  public void testOpen() {
    try {
      File tempFile = TempFile.createTempFile( "temp", "test" );
      String tempFilePath = tempFile.getAbsolutePath();
      String dataFileVfsPath = "file:///" + tempFilePath;
      LocalFile tempFileObject = mock( LocalFile.class );

      tempFile.deleteOnExit();

      doReturn( dataFileVfsPath ).when( oraBulkLoaderMeta ).getDataFile();
      doReturn( tempFilePath ).when( variables ).environmentSubstitute( dataFileVfsPath );
      doReturn( tempFileObject ).when( oraBulkDataOutput ).getFileObject( tempFilePath, variables );
      doReturn( tempFilePath ).when( oraBulkDataOutput ).getFilename( tempFileObject );

      oraBulkDataOutput.open( variables, sqlldrProcess );
      oraBulkDataOutput.close();

    } catch ( Exception ex ) {
      fail( "If any exception occurs, this test fails: " + ex );
    }
  }

  @Test
  public void testOpenFileException() {
    doThrow( IOException.class ).when( oraBulkLoaderMeta ).getDataFile();
    try {
      oraBulkDataOutput.open( variables, sqlldrProcess );
      fail( "An IOException was supposed to be thrown, failing test" );
    } catch ( HopException kex ) {
      assertTrue( kex.getMessage().contains( "IO exception occured:" ) );
    }
  }
}
