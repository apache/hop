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

package org.apache.hop.pipeline.transforms.univariatestats;

import org.apache.commons.io.IOUtils;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnivariateStatsMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private final FieldLoadSaveValidator<UnivariateStatsMetaFunction> univariateFunctionFieldLoadSaveValidator =
    new FieldLoadSaveValidator<UnivariateStatsMetaFunction>() {
      final Random random = new Random();

      @Override
      public boolean validateTestObject( UnivariateStatsMetaFunction testObject, Object actual ) {
        return testObject.getXml().equals( ( (UnivariateStatsMetaFunction) actual ).getXml() );
      }

      @Override
      public UnivariateStatsMetaFunction getTestObject() {
        return new UnivariateStatsMetaFunction( UUID.randomUUID().toString(), random.nextBoolean(), random
          .nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), random.nextBoolean(),
          random.nextDouble(), random.nextBoolean() );
      }
    };
  private final ArrayLoadSaveValidator<UnivariateStatsMetaFunction> univariateFunctionArrayFieldLoadSaveValidator =
    new ArrayLoadSaveValidator<UnivariateStatsMetaFunction>( univariateFunctionFieldLoadSaveValidator );

  @Test
  public void testGetAndSetSetInputFieldMetaFunctions() {
    UnivariateStatsMetaFunction[] stats = new UnivariateStatsMetaFunction[ 3 ];
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    meta.setInputFieldMetaFunctions( stats );
    assertTrue( stats == meta.getInputFieldMetaFunctions() );
  }

  @Test
  public void testAllocateAndGetNumFieldsToProcess() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    meta.allocate( 13 );
    assertEquals( 13, meta.getNumFieldsToProcess() );
  }

  @Test
  public void testLegacyLoadXml() throws IOException, HopXmlException {
    String legacyXml =
      IOUtils.toString( UnivariateStatsMetaTest.class.getClassLoader().getResourceAsStream(
              "org/apache/hop/pipeline/transforms/univariatestats/legacyUnivariateStatsMetaTest.xml") );
    IMetaStore mockMetaStore = mock( IMetaStore.class );
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    meta.loadXml( XmlHandler.loadXMLString( legacyXml ).getFirstChild(), mockMetaStore );
    assertEquals( 2, meta.getNumFieldsToProcess() );
    UnivariateStatsMetaFunction first = meta.getInputFieldMetaFunctions()[ 0 ];
    assertEquals( "a", first.getSourceFieldName() );
    assertEquals( true, first.getCalcN() );
    assertEquals( true, first.getCalcMean() );
    assertEquals( true, first.getCalcStdDev() );
    assertEquals( true, first.getCalcMin() );
    assertEquals( true, first.getCalcMax() );
    assertEquals( true, first.getCalcMedian() );
    assertEquals( .5, first.getCalcPercentile(), 0 );
    assertEquals( true, first.getInterpolatePercentile() );
    UnivariateStatsMetaFunction second = meta.getInputFieldMetaFunctions()[ 1 ];
    assertEquals( "b", second.getSourceFieldName() );
    assertEquals( false, second.getCalcN() );
    assertEquals( false, second.getCalcMean() );
    assertEquals( false, second.getCalcStdDev() );
    assertEquals( false, second.getCalcMin() );
    assertEquals( false, second.getCalcMax() );
    assertEquals( false, second.getCalcMedian() );
    assertEquals( -1.0, second.getCalcPercentile(), 0 );
    assertEquals( false, second.getInterpolatePercentile() );
  }

  @Test
  public void loadSaveRoundTripTest() throws HopException {
    List<String> attributes = Arrays.asList( "inputFieldMetaFunctions" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();

    fieldLoadSaveValidatorTypeMap.put( UnivariateStatsMetaFunction[].class.getCanonicalName(),
      univariateFunctionArrayFieldLoadSaveValidator );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( UnivariateStatsMeta.class, attributes, new HashMap<>(),
        new HashMap<>(), new HashMap<String, FieldLoadSaveValidator<?>>(),
        fieldLoadSaveValidatorTypeMap );

    loadSaveTester.testSerialization();
  }

  private void assertContains( Map<String, Integer> map, String key, Integer value ) {
    assertTrue( "Expected map to contain " + key, map.containsKey( key ) );
    assertTrue( "Expected key of " + key + " to be of type " + ValueMetaBase.getTypeDesc( value ),
      map.get( key ) == value );
  }

  @Test
  public void testGetFields() throws HopTransformException {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    UnivariateStatsMetaFunction[] functions = univariateFunctionArrayFieldLoadSaveValidator.getTestObject();
    meta.setInputFieldMetaFunctions( functions );
    IRowMeta mockRowMetaInterface = mock( IRowMeta.class );
    final AtomicBoolean clearCalled = new AtomicBoolean( false );
    final List<IValueMeta> valueMetaInterfaces = new ArrayList<IValueMeta>();
    doAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        clearCalled.set( true );
        return null;
      }
    } ).when( mockRowMetaInterface ).clear();
    doAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        if ( !clearCalled.get() ) {
          throw new RuntimeException( "Clear not called before adding value metas" );
        }
        valueMetaInterfaces.add( (IValueMeta) invocation.getArguments()[ 0 ] );
        return null;
      }
    } ).when( mockRowMetaInterface ).addValueMeta( any( IValueMeta.class ) );
    meta.getFields( mockRowMetaInterface, null, null, null, null, null );
    Map<String, Integer> valueMetas = new HashMap<String, Integer>();
    for ( IValueMeta vmi : valueMetaInterfaces ) {
      valueMetas.put( vmi.getName(), vmi.getType() );
    }
    for ( UnivariateStatsMetaFunction function : functions ) {
      if ( function.getCalcN() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(N)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcMean() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(mean)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcStdDev() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(stdDev)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcMin() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(min)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcMax() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(max)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcMedian() ) {
        assertContains( valueMetas, function.getSourceFieldName() + "(median)", IValueMeta.TYPE_NUMBER );
      }
      if ( function.getCalcPercentile() >= 0 ) {
        NumberFormat pF = NumberFormat.getInstance();
        pF.setMaximumFractionDigits( 2 );
        String res = pF.format( function.getCalcPercentile() * 100 );
        assertContains( valueMetas, function.getSourceFieldName() + "(" + res + "th percentile)",
          IValueMeta.TYPE_NUMBER );
      }
    }
  }

  @Test
  public void testCheckNullPrev() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    meta.check( remarks, null, null, null, new String[ 0 ], null, null, null, null );
    assertEquals( 2, remarks.size() );
    assertEquals( "Not receiving any fields from previous transforms!", remarks.get( 0 ).getText() );
  }

  @Test
  public void testCheckEmptyPrev() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    IRowMeta mockRowMetaInterface = mock( IRowMeta.class );
    when( mockRowMetaInterface.size() ).thenReturn( 0 );
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    meta.check( remarks, null, null, mockRowMetaInterface, new String[ 0 ], null, null, null, null );
    assertEquals( 2, remarks.size() );
    assertEquals( "Not receiving any fields from previous transforms!", remarks.get( 0 ).getText() );
  }

  @Test
  public void testCheckGoodPrev() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    IRowMeta mockRowMetaInterface = mock( IRowMeta.class );
    when( mockRowMetaInterface.size() ).thenReturn( 500 );
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    meta.check( remarks, null, null, mockRowMetaInterface, new String[ 0 ], null, null, null, null );
    assertEquals( 2, remarks.size() );
    assertEquals( "Transform is connected to previous one, receiving " + 500 + " fields", remarks.get( 0 ).getText() );
  }

  @Test
  public void testCheckWithInput() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    meta.check( remarks, null, null, null, new String[ 1 ], null, null, null, null );
    assertEquals( 2, remarks.size() );
    assertEquals( "Transform is receiving info from other transforms.", remarks.get( 1 ).getText() );
  }

  @Test
  public void testCheckWithoutInput() {
    UnivariateStatsMeta meta = new UnivariateStatsMeta();
    List<CheckResultInterface> remarks = new ArrayList<CheckResultInterface>();
    meta.check( remarks, null, null, null, new String[ 0 ], null, null, null, null );
    assertEquals( 2, remarks.size() );
    assertEquals( "No input received from other transforms!", remarks.get( 1 ).getText() );
  }

  @Test
  public void testGetTransform() {
    TransformMeta mockTransformMeta = mock( TransformMeta.class );
    when( mockTransformMeta.getName() ).thenReturn( "testName" );
    ITransformData mockTransformDataInterface = mock( ITransformData.class );
    int cnr = 10;
    PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
    Pipeline mockPipeline = mock( Pipeline.class );
    when( mockPipelineMeta.findTransform( "testName" ) ).thenReturn( mockTransformMeta );
    ITransform transform =
      new UnivariateStatsMeta().getTransform( mockTransformMeta, mockTransformDataInterface, cnr, mockPipelineMeta, mockPipeline );
    assertTrue( "Expected Transform to be instanceof " + UnivariateStats.class, transform instanceof UnivariateStats );
  }

  @Test
  public void testGetTransformData() {
    assertTrue( "Expected TransformData to be instanceof " + UnivariateStatsData.class, new UnivariateStatsMeta()
      .getTransformData() instanceof UnivariateStatsData );
  }
}
