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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test case for PDI-2875
 *
 * @author Pavel Sakun
 */
public class PDI_2875_Test {
  private static TransformMockHelper<TextFileInputMeta, TextFileInputData> smh;
  private final String VAR_NAME = "VAR";
  private final String EXPRESSION = "${" + VAR_NAME + "}";
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws Exception {
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
    meta.inputFiles.includeSubFolders = new String[] { "n", "n" };
    meta.setFilter( new TextFileFilter[ 0 ] );
    meta.content.fileFormat = "unix";
    meta.content.fileType = "CSV";
    meta.errorHandling.lineNumberFilesDestinationDirectory = EXPRESSION;
    meta.errorHandling.errorFilesDestinationDirectory = EXPRESSION;
    meta.errorHandling.warningFilesDestinationDirectory = EXPRESSION;

    return meta;
  }

  @Test
  public void testVariableSubstitution() {
    doReturn( new Date() ).when( smh.pipeline ).getExecutionStartDate();
    TextFileInputData data = new TextFileInputData();
    TextFileInputMeta meta = getMeta();
    TextFileInput transform = spy( new TextFileInput( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline ) );
    transform.setVariable( VAR_NAME, "value" );
    transform.init();
    verify( transform, times( 2 ) ).resolve( EXPRESSION );
  }
}
