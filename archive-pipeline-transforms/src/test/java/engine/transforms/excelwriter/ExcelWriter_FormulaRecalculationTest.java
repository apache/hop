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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Andrey Khayrutdinov
 */
public class ExcelWriter _FormulaRecalculationTest {

  private ExcelWriter  transform;
  private ExcelWriter Data data;
  private TransformMockHelper<ExcelWriterTransformMeta, ITransformData> mockHelper;

  @Before
  public void setUp() throws Exception {
    mockHelper =
      TransformMockUtil.getTransformMockHelper( ExcelWriterTransformMeta.class, "ExcelWriterTransform_FormulaRecalculationTest" );

    transform = new ExcelWriterTransform(
      mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    transform = spy( transform );
    // ignoring to avoid useless errors in log
    doNothing().when( transform ).prepareNextOutputFile();

    data = new ExcelWriter Data();

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
    transform.setVariable( ExcelWriter.STREAMER_FORCE_RECALC_PROP_NAME, property );
    data.wb = spy( new SXSSFWorkbook() );
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
