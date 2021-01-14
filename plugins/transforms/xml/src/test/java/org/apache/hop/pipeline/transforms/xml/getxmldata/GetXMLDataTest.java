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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import junit.framework.TestCase;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.xml.RowTransformCollector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Test class for the "Get XML Data" transform.
 * 
 * @author Sven Boden
 */
public class GetXMLDataTest extends TestCase {
  public IRowMeta createRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta = { new ValueMetaString( "field1") };

    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rm.addValueMeta( valuesMeta[i] );
    }

    return rm;
  }

  private static String getXML1() {
    String xml1 =
        "<Level1>                                     " + " <Level2>                                    "
            + "  <Props>                                    " + "   <ObjectID>AAAAA</ObjectID>                "
            + "   <SAPIDENT>31-8200</SAPIDENT>              " + "   <Quantity>1</Quantity>                    "
            + "   <Merkmalname>TX_B</Merkmalname>           " + "   <Merkmalswert> 600</Merkmalswert>         "
            + "  </Props>                                   " + "  <Props>                                    "
            + "   <ObjectID>BBBBB</ObjectID>                " + "   <SAPIDENT>31-8201</SAPIDENT>              "
            + "   <Quantity>3</Quantity>                    " + "   <Merkmalname>TX_C</Merkmalname>           "
            + "   <Merkmalswert> 900</Merkmalswert>         " + "  </Props>                                   "
            + " </Level2>                                   " + "</Level1>";
    return xml1;
  }

  private static String getXML2() {
    String xml2 =
        "<Level1>                                 " + " <Level2>                                    "
            + "  <Props>                                    " + "   <ObjectID>CCCCC</ObjectID>                "
            + "   <SAPIDENT>11-8201</SAPIDENT>              " + "   <Quantity>5</Quantity>                    "
            + "   <Merkmalname>TX_C</Merkmalname>           " + "   <Merkmalswert> 700</Merkmalswert>         "
            + "  </Props>                                   " + " </Level2>                                   "
            + "</Level1>";
    return xml2;
  }

  public List<RowMetaAndData> createData() {
    List<RowMetaAndData> list = new ArrayList<>();

    IRowMeta rm = createRowMetaInterface();

    Object[] r1 = new Object[] { getXML1() };
    Object[] r2 = new Object[] { getXML2() };

    list.add( new RowMetaAndData( rm, r1 ) );
    list.add( new RowMetaAndData( rm, r2 ) );

    return list;
  }

  public IRowMeta createResultRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta =
      { new ValueMetaString( "field1"), new ValueMetaString( "objectid"),
        new ValueMetaString( "sapident"), new ValueMetaString( "quantity" ),
        new ValueMetaString( "merkmalname" ), new ValueMetaString( "merkmalswert" ) };

    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rm.addValueMeta( valuesMeta[i] );
    }

    return rm;
  }

  /**
   * Create result data for test case 1.
   * 
   * @return list of metadata/data couples of how the result should look like.
   */
  public List<RowMetaAndData> createResultData1() {
    List<RowMetaAndData> list = new ArrayList<>();

    IRowMeta rm = createResultRowMetaInterface();

    Object[] r1 = new Object[] { getXML1(), "AAAAA", "31-8200", "1", "TX_B", " 600" };
    Object[] r2 = new Object[] { getXML1(), "BBBBB", "31-8201", "3", "TX_C", " 900" };
    Object[] r3 = new Object[] { getXML2(), "CCCCC", "11-8201", "5", "TX_C", " 700" };

    list.add( new RowMetaAndData( rm, r1 ) );
    list.add( new RowMetaAndData( rm, r2 ) );
    list.add( new RowMetaAndData( rm, r3 ) );

    return list;
  }

  /**
   * Check the 2 lists comparing the rows in order. If they are not the same fail the test.
   * 
   * @param rows1
   *          set 1 of rows to compare
   * @param rows2
   *          set 2 of rows to compare
   */
  public void checkRows( List<RowMetaAndData> rows1, List<RowMetaAndData> rows2 ) {
    int idx = 1;
    if ( rows1.size() != rows2.size() ) {
      fail( "Number of rows is not the same: " + rows1.size() + " and " + rows2.size() );
    }
    Iterator<RowMetaAndData> it1 = rows1.iterator();
    Iterator<RowMetaAndData> it2 = rows2.iterator();

    while ( it1.hasNext() && it2.hasNext() ) {
      RowMetaAndData rm1 = it1.next();
      RowMetaAndData rm2 = it2.next();

      Object[] r1 = rm1.getData();
      Object[] r2 = rm2.getData();

      if ( rm1.size() != rm2.size() ) {
        fail( "row nr " + idx + " is not equal" );
      }
      int[] fields = new int[r1.length];
      for ( int ydx = 0; ydx < r1.length; ydx++ ) {
        fields[ydx] = ydx;
      }
      try {
        if ( rm1.getRowMeta().compare( r1, r2, fields ) != 0 ) {
          fail( "row nr " + idx + " is not equal" );
        }
      } catch ( HopValueException e ) {
        fail( "row nr " + idx + " is not equal" );
      }

      idx++;
    }
  }

  /**
   * Test case for Get XML Data transform, very simple example.
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testGetXMLDataSimple1() throws Exception {
    HopEnvironment.init();

    //
    // Create a new transformation...
    //

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "getxmldata1" );

    PluginRegistry registry = PluginRegistry.getInstance();

    //
    // create an injector transform...
    //
    String injectorTransformName = "injector transform";
    InjectorMeta im = new InjectorMeta();

    // Set the information of the injector.
    String injectorPid = registry.getPluginId( TransformPluginType.class, im );
    TransformMeta injectorTransform = new TransformMeta( injectorPid, injectorTransformName, im );
    pipelineMeta.addTransform( injectorTransform );

    //
    // Create a Get XML Data transform
    //
    String getXMLDataName = "get xml data transform";
    GetXmlDataMeta gxdm = new GetXmlDataMeta();

    String getXMLDataPid = registry.getPluginId( TransformPluginType.class, gxdm );
    TransformMeta getXMLDataTransform = new TransformMeta( getXMLDataPid, getXMLDataName, gxdm );
    pipelineMeta.addTransform( getXMLDataTransform );

    GetXmlDataField[] fields = new GetXmlDataField[5];

    for ( int idx = 0; idx < fields.length; idx++ ) {
      fields[idx] = new GetXmlDataField();
    }

    fields[0].setName( "objectid" );
    fields[0].setXPath( "ObjectID" );
    fields[0].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[0].setType( IValueMeta.TYPE_STRING );
    fields[0].setFormat( "" );
    fields[0].setLength( -1 );
    fields[0].setPrecision( -1 );
    fields[0].setCurrencySymbol( "" );
    fields[0].setDecimalSymbol( "" );
    fields[0].setGroupSymbol( "" );
    fields[0].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    fields[1].setName( "sapident" );
    fields[1].setXPath( "SAPIDENT" );
    fields[1].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[1].setType( IValueMeta.TYPE_STRING );
    fields[1].setFormat( "" );
    fields[1].setLength( -1 );
    fields[1].setPrecision( -1 );
    fields[1].setCurrencySymbol( "" );
    fields[1].setDecimalSymbol( "" );
    fields[1].setGroupSymbol( "" );
    fields[1].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    fields[2].setName( "quantity" );
    fields[2].setXPath( "Quantity" );
    fields[2].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[2].setType( IValueMeta.TYPE_STRING );
    fields[2].setFormat( "" );
    fields[2].setLength( -1 );
    fields[2].setPrecision( -1 );
    fields[2].setCurrencySymbol( "" );
    fields[2].setDecimalSymbol( "" );
    fields[2].setGroupSymbol( "" );
    fields[2].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    fields[3].setName( "merkmalname" );
    fields[3].setXPath( "Merkmalname" );
    fields[3].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[3].setType( IValueMeta.TYPE_STRING );
    fields[3].setFormat( "" );
    fields[3].setLength( -1 );
    fields[3].setPrecision( -1 );
    fields[3].setCurrencySymbol( "" );
    fields[3].setDecimalSymbol( "" );
    fields[3].setGroupSymbol( "" );
    fields[3].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    fields[4].setName( "merkmalswert" );
    fields[4].setXPath( "Merkmalswert" );
    fields[4].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[4].setType( IValueMeta.TYPE_STRING );
    fields[4].setFormat( "" );
    fields[4].setLength( -1 );
    fields[4].setPrecision( -1 );
    fields[4].setCurrencySymbol( "" );
    fields[4].setDecimalSymbol( "" );
    fields[4].setGroupSymbol( "" );
    fields[4].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    gxdm.setEncoding( "UTF-8" );
    gxdm.setIsAFile( false );
    gxdm.setInFields( true );
    gxdm.setLoopXPath( "Level1/Level2/Props" );
    gxdm.setXMLField( "field1" );
    gxdm.setInputFields( fields );

    PipelineHopMeta hi = new PipelineHopMeta( injectorTransform, getXMLDataTransform );
    pipelineMeta.addPipelineHop( hi );

    //
    // Create a dummy transform 1
    //
    String dummyTransformName1 = "dummy transform 1";
    DummyMeta dm1 = new DummyMeta();

    String dummyPid1 = registry.getPluginId( TransformPluginType.class, dm1 );
    TransformMeta dummyTransform1 = new TransformMeta( dummyPid1, dummyTransformName1, dm1 );
    pipelineMeta.addTransform( dummyTransform1 );

    PipelineHopMeta hi1 = new PipelineHopMeta( getXMLDataTransform, dummyTransform1 );
    pipelineMeta.addPipelineHop( hi1 );

    // Now execute the transformation...
    Pipeline trans = new LocalPipelineEngine( pipelineMeta );

    trans.prepareExecution();

    ITransform si = trans.getTransformInterface( dummyTransformName1, 0 );
    RowTransformCollector dummyRc1 = new RowTransformCollector();
    si.addRowListener( dummyRc1 );

    RowProducer rp = trans.addRowProducer( injectorTransformName, 0 );
    trans.startThreads();

    // add rows
    List<RowMetaAndData> inputList = createData();
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    trans.waitUntilFinished();

    // Compare the results
    List<RowMetaAndData> resultRows = dummyRc1.getRowsWritten();
    List<RowMetaAndData> goldenImageRows = createResultData1();

    checkRows( goldenImageRows, resultRows );
  }

  public void testInit() throws Exception {

    HopEnvironment.init();

    //
    // Create a new transformation...
    //
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "getxmldata1" );

    PluginRegistry registry = PluginRegistry.getInstance();

    //
    // create an injector transform...
    //
    String injectorTransformName = "injector transform";
    InjectorMeta im = new InjectorMeta();

    // Set the information of the injector.
    String injectorPid = registry.getPluginId( TransformPluginType.class, im );
    TransformMeta injectorTransform = new TransformMeta( injectorPid, injectorTransformName, im );
    pipelineMeta.addTransform( injectorTransform );

    //
    // Create a Get XML Data transform
    //
    String getXMLDataName = "get xml data transform";
    GetXmlDataMeta gxdm = new GetXmlDataMeta();

    String getXMLDataPid = registry.getPluginId( TransformPluginType.class, gxdm );
    TransformMeta getXMLDataTransform = new TransformMeta( getXMLDataPid, getXMLDataName, gxdm );
    pipelineMeta.addTransform( getXMLDataTransform );

    GetXmlDataField[] fields = new GetXmlDataField[5];

    for ( int idx = 0; idx < fields.length; idx++ ) {
      fields[idx] = new GetXmlDataField();
    }

    fields[0].setName( "objectid" );
    fields[0].setXPath( "${xml_path}" );
    fields[0].setElementType( GetXmlDataField.ELEMENT_TYPE_NODE );
    fields[0].setType( IValueMeta.TYPE_STRING );
    fields[0].setFormat( "" );
    fields[0].setLength( -1 );
    fields[0].setPrecision( -1 );
    fields[0].setCurrencySymbol( "" );
    fields[0].setDecimalSymbol( "" );
    fields[0].setGroupSymbol( "" );
    fields[0].setTrimType( GetXmlDataField.TYPE_TRIM_NONE );

    gxdm.setEncoding( "UTF-8" );
    gxdm.setIsAFile( false );
    gxdm.setInFields( true );
    gxdm.setLoopXPath( "Level1/Level2/Props" );
    gxdm.setXMLField( "field1" );
    gxdm.setInputFields( fields );

    PipelineHopMeta hi = new PipelineHopMeta( injectorTransform, getXMLDataTransform );
    pipelineMeta.addPipelineHop( hi );

    //
    // Create a dummy transform 1
    //
    String dummyTransformName1 = "dummy transform 1";
    DummyMeta dm1 = new DummyMeta();

    String dummyPid1 = registry.getPluginId( TransformPluginType.class, dm1 );
    TransformMeta dummyTransform1 = new TransformMeta( dummyPid1, dummyTransformName1, dm1 );
    pipelineMeta.addTransform( dummyTransform1 );

    PipelineHopMeta hi1 = new PipelineHopMeta( getXMLDataTransform, dummyTransform1 );
    pipelineMeta.addPipelineHop( hi1 );

    // Now execute the transformation...
    Pipeline trans = new LocalPipelineEngine( pipelineMeta );

    trans.prepareExecution(  );

    ITransform si = trans.getTransformInterface( dummyTransformName1, 0 );
    RowTransformCollector dummyRc1 = new RowTransformCollector();
    si.addRowListener( dummyRc1 );

    RowProducer rp = trans.addRowProducer( injectorTransformName, 0 );
    trans.startThreads();

    // add rows
    List<RowMetaAndData> inputList = createData();
    Iterator<RowMetaAndData> it = inputList.iterator();
    while ( it.hasNext() ) {
      RowMetaAndData rm = it.next();
      rp.putRow( rm.getRowMeta(), rm.getData() );
    }
    rp.finished();

    trans.waitUntilFinished();

    // Compare the results
    List<RowMetaAndData> resultRows = dummyRc1.getRowsWritten();
    List<RowMetaAndData> goldenImageRows = createResultData1();

    GetXmlDataData getXMLDataData = new GetXmlDataData();
    GetXmlData getXmlData = new GetXmlData( dummyTransform1, gxdm, getXMLDataData, 0, pipelineMeta, trans );
    getXmlData.setVariable( "xml_path", "data/owner" );
    getXmlData.init();
    assertEquals( "${xml_path}", gxdm.getInputFields()[0].getXPath() );
    assertEquals( "data/owner", gxdm.getInputFields()[0].getResolvedXPath() );
  }
}
