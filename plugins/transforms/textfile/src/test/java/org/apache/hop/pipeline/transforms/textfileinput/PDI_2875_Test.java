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

package org.apache.hop.pipeline.transforms.textfileinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.fileinput.TextFileFilter;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputData;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test case for PDI-2875
 *
 * @author Pavel Sakun
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
public class PDI_2875_Test {
  private static TransformMockHelper<TextFileInputMeta, TextFileInputData> smh;
  private final String VAR_NAME = "VAR";
  private final String EXPRESSION = "${" + VAR_NAME + "}";
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws HopException {
    HopEnvironment.init();
    smh =
      new TransformMockHelper<>( "CsvInputTest", TextFileInputMeta.class, TextFileInputData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) )
      .thenReturn( smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    smh.cleanUp();
  }

  private TextFileInputMeta getMeta() {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.allocateFiles( 2 );
    meta.setFileName( new String[] { "file1.txt", "file2.txt" } );
    meta.setIncludeSubFolders( new String[] { "n", "n" } );
    meta.setFilter( new TextFileFilter[ 0 ] );
    meta.setFileFormat( "unix" );
    meta.setFileType( "CSV" );
    meta.setLineNumberFilesDestinationDirectory( EXPRESSION );
    meta.setErrorFilesDestinationDirectory( EXPRESSION );
    meta.setWarningFilesDestinationDirectory( EXPRESSION );

    return meta;
  }
}
