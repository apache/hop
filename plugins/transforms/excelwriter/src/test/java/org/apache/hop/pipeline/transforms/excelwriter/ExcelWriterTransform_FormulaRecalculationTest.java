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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class ExcelWriterTransform_FormulaRecalculationTest {

  private ExcelWriterTransform transform;
  private ExcelWriterTransformMeta meta;
  private ExcelWriterTransformData data;
  private TransformMockHelper<ExcelWriterTransformMeta, ExcelWriterTransformData> mockHelper;

  @Before
  public void setUp() throws Exception {
    mockHelper = TransformMockUtil.getTransformMockHelper( ExcelWriterTransformMeta.class, ExcelWriterTransformData.class, "ExcelWriterTransform_FormulaRecalculationTest" );

    meta = mockHelper.iTransformMeta;

    ExcelWriterFileField fieldMock = mock( ExcelWriterFileField.class );
    doReturn(fieldMock).when(meta).getFile();

    ExcelWriterTemplateField templateMock = mock( ExcelWriterTemplateField.class );
    doReturn(templateMock).when(meta).getTemplate();

    data = new ExcelWriterTransformData();
  }

  private void setupTransform() throws Exception {
    transform = new ExcelWriterTransform( mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    transform = spy( transform );
    // ignoring to avoid useless errors in log
    doNothing().when( transform ).prepareNextOutputFile();
    transform.init();
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void forcesToRecalculate_Sxssf_PropertyIsSet() throws Exception {
    forcesToRecalculate_Sxssf( "Y", true );
  }

  @Test
  public void forcesToRecalculate_Sxssf_PropertyIsCleared() throws Exception {
    forcesToRecalculate_Sxssf( "N", false );
  }

  @Test
  public void forcesToRecalculate_Sxssf_PropertyIsNotSet() throws Exception {
    forcesToRecalculate_Sxssf( null, false );
  }

  private void forcesToRecalculate_Sxssf( String property, boolean expectedFlag ) throws Exception {
    data.wb = spy( new SXSSFWorkbook() );

    setupTransform();

    transform.setVariable( ExcelWriterTransform.STREAMER_FORCE_RECALC_PROP_NAME, property );
    transform.recalculateAllWorkbookFormulas();
    if ( expectedFlag ) {
      verify( data.wb ).setForceFormulaRecalculation( true );
    } else {
      verify( data.wb, never() ).setForceFormulaRecalculation( anyBoolean() );
    }
  }

  @Test
  public void forcesToRecalculate_Hssf() throws Exception {
    data.wb = new HSSFWorkbook();
    data.wb.createSheet( "sheet1" );
    data.wb.createSheet( "sheet2" );

    setupTransform();
    transform.recalculateAllWorkbookFormulas();

    if ( !data.wb.getForceFormulaRecalculation() ) {
      int sheets = data.wb.getNumberOfSheets();
      for ( int i = 0; i < sheets; i++ ) {
        Sheet sheet = data.wb.getSheetAt( i );
        assertTrue( "Sheet #" + i + ": " + sheet.getSheetName(), sheet.getForceFormulaRecalculation() );
      }
    }
  }
}
