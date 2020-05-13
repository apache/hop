/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.selectvalues;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.selectvalues.SelectValuesMeta.SelectField;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Note: In Europe (e.g. in UK), week starts on Monday. In USA, it starts on Sunday.
 *
 * @author Andrey Khayrutdinov
 */
public class SelectValues_LocaleHandling_Test {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private SelectValues transform;
  private Locale current;
  private TransformMockHelper<SelectValuesMeta, ITransformData> helper;

  @Before
  public void setUp() throws Exception {
    current = Locale.getDefault();
    Locale.setDefault( Locale.UK );

    helper =
      TransformMockUtil.getTransformMockHelper( SelectValuesMeta.class, "SelectValues_LocaleHandling_Test" );
    when( helper.transformMeta.isDoingErrorHandling() ).thenReturn( true );

    transform = new SelectValues( helper.transformMeta, helper.iTransformData, 1, helper.pipelineMeta, helper.pipeline );
    transform = spy( transform );

    // Dec 28, 2015
    Calendar calendar = Calendar.getInstance();
    calendar.set( 2015, Calendar.DECEMBER, 28, 0, 0, 0 );
    doReturn( new Object[] { calendar.getTime() } ).doReturn( null )
      .when( transform ).getRow();
  }

  @After
  public void tearDown() throws Exception {
    transform = null;

    Locale.setDefault( current );
    current = null;

    helper.cleanUp();
  }


  @Test
  public void returns53_ForNull() throws Exception {
    executeAndCheck( null, "53" );
  }

  @Test
  public void returns53_ForEmpty() throws Exception {
    executeAndCheck( "", "53" );
  }

  @Test
  public void returns53_ForEn_GB() throws Exception {
    executeAndCheck( "en_GB", "53" );
  }

  @Test
  public void returns01_ForEn_US() throws Exception {
    executeAndCheck( "en_US", "01" );
  }

  private void executeAndCheck( String locale, String expectedWeekNumber ) throws Exception {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( new ValueMetaDate( "field" ) );
    transform.setInputRowMeta( inputRowMeta );

    SelectValuesMeta transformMeta = new SelectValuesMeta();
    transformMeta.allocate( 1, 0, 1 );
    transformMeta.getSelectFields()[ 0 ] = new SelectField();
    transformMeta.getSelectFields()[ 0 ].setName( "field" );
    transformMeta.getMeta()[ 0 ] =
      new SelectMetadataChange( transformMeta, "field", null, IValueMeta.TYPE_STRING, -2, -2,
        IValueMeta.STORAGE_TYPE_NORMAL, "ww", false, locale, null, false, null, null, null );

    SelectValuesData transformData = new SelectValuesData();
    transformData.select = true;
    transformData.metadata = true;
    transformData.firstselect = true;
    transformData.firstmetadata = true;

    List<Object[]> execute = PipelineTestingUtil.execute( transform, transformMeta, transformData, 1, true );
    PipelineTestingUtil.assertResult( execute, Collections.singletonList( new Object[] { expectedWeekNumber } ) );
  }
}
