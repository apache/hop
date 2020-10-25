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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SwitchCaseTest {

  private TransformMockHelper<SwitchCaseMeta, SwitchCaseData> mockHelper;
  private static Boolean EMPTY_STRING_AND_NULL_ARE_DIFFERENT = false;

  @Before
  public void setUp() throws Exception {
    mockHelper =
      new TransformMockHelper<SwitchCaseMeta, SwitchCaseData>(
        "Switch Case", SwitchCaseMeta.class, SwitchCaseData.class );
    when( mockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      mockHelper.logChannelInterface );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() throws Exception {
    mockHelper.cleanUp();
  }

  /**
   * PDI 6900. Test that process row works correctly. Simulate transform workload when input and output row sets already
   * created and mapped to specified case values.
   *
   * @throws HopException
   */
  @Test
  public void testProcessRow() throws HopException {
    SwitchCaseCustom krasavez = new SwitchCaseCustom( mockHelper );
    krasavez.first = false;

    // create two output row sets
    RowSet rowSetOne = new QueueRowSet();
    RowSet rowSetTwo = new QueueRowSet();

    // this row set should contain only '3'.
    krasavez.data.outputMap.put( 3, rowSetOne );
    krasavez.data.outputMap.put( 3, rowSetTwo );

    // this row set contains nulls only
    RowSet rowSetNullOne = new QueueRowSet();
    RowSet rowSetNullTwo = new QueueRowSet();
    krasavez.data.nullRowSetSet.add( rowSetNullOne );
    krasavez.data.nullRowSetSet.add( rowSetNullTwo );

    // this row set contains all expect null or '3'
    RowSet def = new QueueRowSet();
    krasavez.data.defaultRowSetSet.add( def );

    // generate some data (see method implementation)
    // expected: 5 times null,
    // expected 1*2 = 2 times 3
    // expected 5*2 + 5 = 15 rows generated
    // expected 15 - 5 - 2 = 8 rows go to default.
    // expected one empty string at the end
    // 1, 1, null, 2, 2, null, 3, 3, null, 4, 4, null, 5, 5, null,""
    krasavez.generateData( 1, 5, 2 );

    // call method under test
    krasavez.processRow();

    assertEquals( "First row set collects 2 rows", 2, rowSetOne.size() );
    assertEquals( "Second row set collects 2 rows", 2, rowSetTwo.size() );

    assertEquals( "First null row set collects 5 rows", 6, rowSetNullOne.size() );
    assertEquals( "Second null row set collects 5 rows", 6, rowSetNullTwo.size() );

    assertEquals( "Default row set collects the rest of rows", 8, def.size() );

    // now - check the data is correct in every row set:
    assertEquals( "First row set contains only 3: ", true, isRowSetContainsValue(
      rowSetOne, new Object[] { 3 }, new Object[] {} ) );
    assertEquals( "Second row set contains only 3: ", true, isRowSetContainsValue(
      rowSetTwo, new Object[] { 3 }, new Object[] {} ) );

    assertEquals( "First null row set contains only null: ", true, isRowSetContainsValue(
      rowSetNullOne, new Object[] { null }, new Object[] {} ) );
    assertEquals( "Second null row set contains only null: ", true, isRowSetContainsValue(
      rowSetNullTwo, new Object[] { null }, new Object[] {} ) );

    assertEquals( "Default row set do not contains null or 3, but other", true, isRowSetContainsValue(
      def, new Object[] { 1, 2, 4, 5 }, new Object[] { 3, null } ) );
  }

  private boolean isRowSetContainsValue( RowSet rowSet, Object[] allowed, Object[] illegal ) {
    boolean ok = true;

    Set<Object> yes = new HashSet<Object>();
    yes.addAll( Arrays.asList( allowed ) );
    Set<Object> no = new HashSet<Object>();
    no.addAll( Arrays.asList( illegal ) );

    for ( int i = 0; i < rowSet.size(); i++ ) {
      Object[] row = rowSet.getRow();
      Object val = row[ 0 ];
      ok = yes.contains( val ) && !no.contains( val );
      if ( !ok ) {
        // this is not ok now
        return false;
      }
    }
    return ok;
  }

  /**
   * PDI-6900 Check that SwichCase transform can correctly set up input values to output rowsets.
   *
   * @throws HopException
   * @throws URISyntaxException
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  @Test
  public void testCreateOutputValueMapping() throws HopException, URISyntaxException,
    ParserConfigurationException, SAXException, IOException {
    SwitchCaseCustom krasavez = new SwitchCaseCustom( mockHelper );

    // load transform info value-case mapping from xml.
    List<DatabaseMeta> emptyList = new ArrayList<DatabaseMeta>();
    krasavez.meta.loadXml( loadTransformXmlMetadata( "SwitchCaseTest.xml" ), mock( IHopMetadataProvider.class ) );

    KeyToRowSetMap expectedNN = new KeyToRowSetMap();
    Set<RowSet> nulls = new HashSet<RowSet>();

    // create real transforms for all targets
    List<SwitchCaseTarget> list = krasavez.meta.getCaseTargets();
    for ( SwitchCaseTarget item : list ) {
      ITransform smInt = new DummyMeta();
      TransformMeta transformMeta = new TransformMeta( item.caseTargetTransformName, smInt );
      item.caseTargetTransform = transformMeta;

      // create and put row set for this
      RowSet rw = new QueueRowSet();
      krasavez.map.put( item.caseTargetTransformName, rw );

      // null values goes to null rowset
      if ( item.caseValue != null ) {
        expectedNN.put( item.caseValue, rw );
      } else {
        nulls.add( rw );
      }
    }

    // create default transform
    ITransform smInt = new DummyMeta();
    TransformMeta transformMeta = new TransformMeta( krasavez.meta.getDefaultTargetTransformName(), smInt );
    krasavez.meta.setDefaultTargetTransform( transformMeta );
    RowSet rw = new QueueRowSet();
    krasavez.map.put( krasavez.meta.getDefaultTargetTransformName(), rw );

    krasavez.createOutputValueMapping();

    // inspect transform output data:
    Set<RowSet> ones = krasavez.data.outputMap.get( "1" );
    assertEquals( "Output map for 1 values contains 2 row sets", 2, ones.size() );

    Set<RowSet> twos = krasavez.data.outputMap.get( "2" );
    assertEquals( "Output map for 2 values contains 1 row sets", 1, twos.size() );

    assertEquals( "Null row set contains 2 items: ", 2, krasavez.data.nullRowSetSet.size() );
    assertEquals( "We have at least one default rowset", 1, krasavez.data.defaultRowSetSet.size() );

    // check that rowsets data is correct:
    Set<RowSet> rowsets = expectedNN.get( "1" );
    for ( RowSet rowset : rowsets ) {
      assertTrue( "Output map for 1 values contains expected row set", ones.contains( rowset ) );
    }
    rowsets = expectedNN.get( "2" );
    for ( RowSet rowset : rowsets ) {
      assertTrue( "Output map for 2 values contains expected row set", twos.contains( rowset ) );
    }
    for ( RowSet rowset : krasavez.data.nullRowSetSet ) {
      assertTrue( "Output map for null values contains expected row set", nulls.contains( rowset ) );
    }
    // we have already check that there is only one item.
    for ( RowSet rowset : krasavez.data.defaultRowSetSet ) {
      assertTrue( "Output map for default case contains expected row set", rowset.equals( rw ) );
    }
  }

  @Test
  public void testCreateOutputValueMappingWithBinaryType() throws HopException, URISyntaxException,
    ParserConfigurationException, SAXException, IOException {
    SwitchCaseCustom krasavez = new SwitchCaseCustom( mockHelper );

    // load transform info value-case mapping from xml.
    List<DatabaseMeta> emptyList = new ArrayList<DatabaseMeta>();
    krasavez.meta.loadXml( loadTransformXmlMetadata( "SwitchCaseBinaryTest.xml" ), mock( IHopMetadataProvider.class ) );

    KeyToRowSetMap expectedNN = new KeyToRowSetMap();
    Set<RowSet> nulls = new HashSet<RowSet>();

    // create real transforms for all targets
    List<SwitchCaseTarget> list = krasavez.meta.getCaseTargets();
    for ( SwitchCaseTarget item : list ) {
      ITransform smInt = new DummyMeta();
      TransformMeta transformMeta = new TransformMeta( item.caseTargetTransformName, smInt );
      item.caseTargetTransform = transformMeta;

      // create and put row set for this
      RowSet rw = new QueueRowSet();
      krasavez.map.put( item.caseTargetTransformName, rw );

      // null values goes to null rowset
      if ( item.caseValue != null ) {
        expectedNN.put( item.caseValue, rw );
      } else {
        nulls.add( rw );
      }
    }

    // create default transform
    ITransform smInt = new DummyMeta();
    TransformMeta transformMeta = new TransformMeta( krasavez.meta.getDefaultTargetTransformName(), smInt );
    krasavez.meta.setDefaultTargetTransform( transformMeta );
    RowSet rw = new QueueRowSet();
    krasavez.map.put( krasavez.meta.getDefaultTargetTransformName(), rw );

    krasavez.createOutputValueMapping();

    // inspect transform output data:
    Set<RowSet> ones = krasavez.data.outputMap.get( "1" );
    assertEquals( "Output map for 1 values contains 2 row sets", 2, ones.size() );

    Set<RowSet> zeros = krasavez.data.outputMap.get( "0" );
    assertEquals( "Output map for 0 values contains 1 row sets", 1, zeros.size() );

    assertEquals( "Null row set contains 0 items: ", 2, krasavez.data.nullRowSetSet.size() );
    assertEquals( "We have at least one default rowset", 1, krasavez.data.defaultRowSetSet.size() );

    // check that rowsets data is correct:
    Set<RowSet> rowsets = expectedNN.get( "1" );
    for ( RowSet rowset : rowsets ) {
      assertTrue( "Output map for 1 values contains expected row set", ones.contains( rowset ) );
    }
    rowsets = expectedNN.get( "0" );
    for ( RowSet rowset : rowsets ) {
      assertTrue( "Output map for 0 values contains expected row set", zeros.contains( rowset ) );
    }
    for ( RowSet rowset : krasavez.data.nullRowSetSet ) {
      assertTrue( "Output map for null values contains expected row set", nulls.contains( rowset ) );
    }
    // we have already check that there is only one item.
    for ( RowSet rowset : krasavez.data.defaultRowSetSet ) {
      assertTrue( "Output map for default case contains expected row set", rowset.equals( rw ) );
    }
  }

  /**
   * Load local xml data for case-value mapping, transform info.
   *
   * @return
   * @throws URISyntaxException
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  private static Node loadTransformXmlMetadata( String fileName ) throws URISyntaxException, ParserConfigurationException, SAXException, IOException {
    String PKG = SwitchCaseTest.class.getPackage().getName().replace( ".", "/" );
    PKG = PKG + "/";
    URL url = SwitchCaseTest.class.getClassLoader().getResource( PKG + fileName );
    File file = new File( url.toURI() );
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse( file );
    NodeList nList = doc.getElementsByTagName( "transform" );
    return nList.item( 0 );
  }


  @Test
  public void processRow_NullsArePutIntoDefaultWhenNotSpecified() throws Exception {
    SwitchCaseCustom transform = new SwitchCaseCustom( mockHelper );
    transform.meta.loadXml( loadTransformXmlMetadata( "SwitchCaseTest_PDI-12671.xml" ), mock( IHopMetadataProvider.class ) );

    List<RowSet> outputRowSets = new LinkedList<RowSet>();
    for ( SwitchCaseTarget item : transform.meta.getCaseTargets() ) {
      ITransform smInt = new DummyMeta();
      item.caseTargetTransform = new TransformMeta( item.caseTargetTransformName, smInt );

      RowSet rw = new QueueRowSet();
      transform.map.put( item.caseTargetTransformName, rw );
      outputRowSets.add( rw );
    }

    // create a default transform
    ITransform smInt = new DummyMeta();
    TransformMeta transformMeta = new TransformMeta( transform.meta.getDefaultTargetTransformName(), smInt );
    transform.meta.setDefaultTargetTransform( transformMeta );
    RowSet defaultRowSet = new QueueRowSet();
    transform.map.put( transform.meta.getDefaultTargetTransformName(), defaultRowSet );

    transform.input.add( new Object[] { null } );
    transform.processRow();

    assertEquals( 1, defaultRowSet.size() );
    for ( RowSet rowSet : outputRowSets ) {
      assertEquals( 0, rowSet.size() );
    }

    assertNull( defaultRowSet.getRow()[ 0 ] );
  }

  @Test
  public void prepareObjectTypeBinaryTest_Equals() throws Exception {
    assertEquals( Arrays.hashCode( new byte[] { 1, 2, 3 } ), SwitchCase.prepareObjectType( new byte[] { 1, 2, 3 } ) );
  }

  @Test
  public void prepareObjectTypeBinaryTest_NotEquals() throws Exception {
    assertNotEquals( Arrays.hashCode( new byte[] { 1, 2, 4 } ), SwitchCase.prepareObjectType( new byte[] { 1, 2, 3 } ) );
  }

  @Test
  public void prepareObjectTypeBinaryTest_Null() throws Exception {
    byte[] given = null;
    byte[] expected = null;

    assertEquals( expected, SwitchCase.prepareObjectType( given ) );
  }

  @Test
  public void prepareObjectTypeTest_Equals() throws Exception {
    assertEquals( "2", SwitchCase.prepareObjectType( "2" ) );
  }

  @Test
  public void prepareObjectTypeTest_NotEquals() throws Exception {
    assertNotEquals( "2", SwitchCase.prepareObjectType( "1" ) );
  }

  @Test
  public void prepareObjectTypeTest_Null() throws Exception {
    assertEquals( null, SwitchCase.prepareObjectType( null ) );
  }

  /**
   * Switch case transform ancestor with overridden methods to have ability to simulate normal pipeline execution.
   */
  private static class SwitchCaseCustom extends SwitchCase {

    Queue<Object[]> input = new LinkedList<Object[]>();
    IRowMeta iRowMeta;

    // we will use real data and meta.
    SwitchCaseData data = new SwitchCaseData();
    SwitchCaseMeta meta = new SwitchCaseMeta();

    Map<String, RowSet> map = new HashMap<String, RowSet>();

    SwitchCaseCustom( TransformMockHelper<SwitchCaseMeta, SwitchCaseData> mockHelper ) throws HopValueException {
      super( mockHelper.transformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
      // this.mockHelper = mockHelper;
     .init();

      // call to convert value will returns same value.
      data.valueMeta = mock( IValueMeta.class );
      when( data.valueMeta.convertData( any( IValueMeta.class ), any() ) ).thenAnswer(
        new Answer<Object>() {
          @Override
          public Object answer( InvocationOnMock invocation ) throws Throwable {
            Object[] objArr = invocation.getArguments();
            return ( objArr != null && objArr.length > 1 ) ? objArr[ 1 ] : null;
          }
        } );
      // same when call to convertDataFromString
      when( data.valueMeta.convertDataFromString( Mockito.anyString(), any( IValueMeta.class ),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyInt() ) ).thenAnswer(
        //CHECKSTYLE:Indentation:OFF
        new Answer<Object>() {
          public Object answer( InvocationOnMock invocation ) throws Throwable {
            Object[] objArr = invocation.getArguments();
            return ( objArr != null && objArr.length > 1 ) ? objArr[ 0 ] : null;
          }
        } );
      // null-check
      when( data.valueMeta.isNull( any() ) ).thenAnswer( new Answer<Object>() {
        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable {
          Object[] objArr = invocation.getArguments();
          Object obj = objArr[ 0 ];
          if ( obj == null ) {
            return true;
          }
          if ( EMPTY_STRING_AND_NULL_ARE_DIFFERENT ) {
            return false;
          }

          // If it's a string and the string is empty, it's a null value as well
          //
          if ( obj instanceof String ) {
            if ( ( (String) obj ).length() == 0 ) {
              return true;
            }
          }
          return false;
        }
      } );

    }

    /**
     * used for input row generation
     *
     * @param start
     * @param finish
     * @param copy
     */
    void generateData( int start, int finish, int copy ) {
      input.clear();
      for ( int i = start; i <= finish; i++ ) {
        for ( int j = 0; j < copy; j++ ) {
          input.add( new Object[] { i } );
        }
        input.add( new Object[] { null } );
      }
      input.add( new Object[] { "" } );
    }

    /**
     * useful to see generated data as String
     *
     * @return
     */
    @SuppressWarnings( "unused" )
    public String getInputDataOverview() {
      StringBuilder sb = new StringBuilder();
      for ( Object[] row : input ) {
        sb.append( row[ 0 ] + ", " );
      }
      return sb.toString();
    }

    /**
     * mock transform data processing
     */
    @Override
    public Object[] getRow() throws HopException {
      return input.poll();
    }

    /**
     * simulate concurrent execution
     *
     * @throws HopException
     */
    public void processRow() throws HopException {
      boolean run = false;
      do {
        run =.init();
      } while ( run );
    }

    @Override
    public RowSet findOutputRowSet( String targetTransform ) throws HopTransformException {
      return map.get( targetTransform );
    }

    @Override
    public IRowMeta getInputRowMeta() {
      if ( iRowMeta == null ) {
        iRowMeta = getDynamicRowMetaInterface();
      }
      return iRowMeta;
    }

    private IRowMeta getDynamicRowMetaInterface() {
      IRowMeta inputRowMeta = mock( IRowMeta.class );
      return inputRowMeta;
    }
  }
}
