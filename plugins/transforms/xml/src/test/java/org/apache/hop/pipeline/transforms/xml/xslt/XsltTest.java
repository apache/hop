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

package org.apache.hop.pipeline.transforms.xml.xslt;

import junit.framework.TestCase;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.file.TextFileInputField;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class XsltTest extends TestCase {

  private static final String TEST1_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><message>Yep, it worked!</message>";

  private static final String TEST1_XSL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<xsl:stylesheet version = \"1.0\" xmlns:xsl = \"http://www.w3.org/1999/XSL/Transform\">"
      + "<xsl:output method = \"text\" encoding = \"UTF-8\"/>" + "<!--simply copy the message to the result tree -->"
      + "<xsl:template match = \"/\">" + "<xsl:value-of select = \"message\"/>" + "</xsl:template>"
      + "</xsl:stylesheet>";

  private static final String TEST1_FNAME = "template.xsl";

  /**
   * Write the file to be used as input (as a temporary file).
   * 
   * @return Absolute file name/path of the created file.
   * @throws IOException
   *           UPON
   */
  public String writeInputFile() throws IOException {

    String rcode = null;

    File tempFile = File.createTempFile( "template", ".xsl" );
    tempFile.deleteOnExit();

    rcode = tempFile.getAbsolutePath();

    FileWriter fout = new FileWriter( tempFile );
    fout.write( TEST1_XSL );
    fout.close();

    return rcode;
  }

  public IRowMeta createRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta =
        { new ValueMetaString( "XML" ), new ValueMetaString( "XSL" ),
          new ValueMetaString( "filename" ), };

    for ( int i = 0; i < valuesMeta.length; i++ ) {
      rm.addValueMeta( valuesMeta[i] );
    }

    return rm;
  }

  public List<RowMetaAndData> createData( String fileName ) {
    List<RowMetaAndData> list = new ArrayList<>();

    IRowMeta rm = createRowMetaInterface();

    Object[] r1 = new Object[] { TEST1_XML, TEST1_XSL, fileName };

    list.add( new RowMetaAndData( rm, r1 ) );

    return list;
  }

  public IRowMeta createResultRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta =
        { new ValueMetaString( "XML" ), new ValueMetaString( "XSL" ),
          new ValueMetaString( "filename" ), new ValueMetaString( "result" ), };

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

    Object[] r1 = new Object[] { TEST1_XML, TEST1_XSL, TEST1_FNAME, "Yep, it worked!" };

    list.add( new RowMetaAndData( rm, r1 ) );

    return list;
  }

  /**
   * Check the 2 lists comparing the rows in order. If they are not the same fail the test.
   * 
   * @param rows1
   *          set 1 of rows to compare
   * @param rows2
   *          set 2 of rows to compare
   * @param fileNameColumn
   *          Number of the column containing the filename. This is only checked for being non-null (some systems maybe
   *          canonize names differently than we input).
   */
  public void checkRows( List<RowMetaAndData> rows1, List<RowMetaAndData> rows2, int fileNameColumn ) {
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
        r1[fileNameColumn] = r2[fileNameColumn];
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
   * Test case for XSLT transform, getting the filename from a field, JAXP factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt1() throws Exception {

    String fileName = writeInputFile();
    runTestWithParams( "XML", "result", true, true, "filename", fileName, "JAXP" );
  }

  /**
   * Test case for XSLT transform, getting the filename from a field, SAXON factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt2() throws Exception {

    String fileName = writeInputFile();
    runTestWithParams( "XML", "result", true, true, "filename", fileName, "SAXON" );
  }

  /**
   * Test case for XSLT transform, getting the XSL from a field, JAXP factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt3() throws Exception {
    runTestWithParams( "XML", "result", true, false, "XSL", "", "JAXP" );
  }

  /**
   * Test case for XSLT transform, getting the XSL from a field, SAXON factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt4() throws Exception {
    runTestWithParams( "XML", "result", true, false, "XSL", "", "SAXON" );
  }

  /**
   * Test case for XSLT transform, getting the XSL from a file, JAXP factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt5() throws Exception {
    String fileName = writeInputFile();
    runTestWithParams( "XML", "result", false, false, "filename", fileName, "JAXP" );
  }

  /**
   * Test case for XSLT transform, getting the XSL from a file, SAXON factory
   * 
   * @throws Exception
   *           Upon any exception
   */
  public void testXslt6() throws Exception {
    String fileName = writeInputFile();
    runTestWithParams( "XML", "result", false, false, "filename", fileName, "SAXON" );
  }

  public void runTestWithParams( String xmlFieldname, String resultFieldname, boolean xslInField,
      boolean xslFileInField, String xslFileField, String xslFilename, String xslFactory ) throws Exception {

    HopEnvironment.init();

    //
    // Create a new transformation...
    //
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "xslt" );

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
    // Create a XSLT transform
    //
    String xsltName = "xslt transform";
    XsltMeta xm = new XsltMeta();

    String xsltPid = registry.getPluginId( TransformPluginType.class, xm );
    TransformMeta xsltTransform = new TransformMeta( xsltPid, xsltName, xm );
    pipelineMeta.addTransform( xsltTransform );

    TextFileInputField[] fields = new TextFileInputField[3];

    for ( int idx = 0; idx < fields.length; idx++ ) {
      fields[idx] = new TextFileInputField();
    }

    fields[0].setName( "XML" );
    fields[0].setType( IValueMeta.TYPE_STRING );
    fields[0].setFormat( "" );
    fields[0].setLength( -1 );
    fields[0].setPrecision( -1 );
    fields[0].setCurrencySymbol( "" );
    fields[0].setDecimalSymbol( "" );
    fields[0].setGroupSymbol( "" );
    fields[0].setTrimType( IValueMeta.TRIM_TYPE_NONE );

    fields[1].setName( "XSL" );
    fields[1].setType( IValueMeta.TYPE_STRING );
    fields[1].setFormat( "" );
    fields[1].setLength( -1 );
    fields[1].setPrecision( -1 );
    fields[1].setCurrencySymbol( "" );
    fields[1].setDecimalSymbol( "" );
    fields[1].setGroupSymbol( "" );
    fields[1].setTrimType( IValueMeta.TRIM_TYPE_NONE );

    fields[2].setName( "filename" );
    fields[2].setType( IValueMeta.TYPE_STRING );
    fields[2].setFormat( "" );
    fields[2].setLength( -1 );
    fields[2].setPrecision( -1 );
    fields[2].setCurrencySymbol( "" );
    fields[2].setDecimalSymbol( "" );
    fields[2].setGroupSymbol( "" );
    fields[2].setTrimType( IValueMeta.TRIM_TYPE_NONE );

    xm.setFieldname( xmlFieldname );
    xm.setResultfieldname( resultFieldname );
    xm.setXSLField( xslInField );
    xm.setXSLFileField( xslFileField );
    xm.setXSLFieldIsAFile( xslFileInField );
    xm.setXslFilename( xslFilename );
    xm.setXSLFactory( xslFactory );

    PipelineHopMeta hi = new PipelineHopMeta( injectorTransform, xsltTransform );
    pipelineMeta.addPipelineHop( hi );

    //
    // Create a dummy transform 1
    //
    String dummyTransformName1 = "dummy transform 1";
    DummyMeta dm1 = new DummyMeta();

    String dummyPid1 = registry.getPluginId( TransformPluginType.class, dm1 );
    TransformMeta dummyTransform1 = new TransformMeta( dummyPid1, dummyTransformName1, dm1 );
    pipelineMeta.addTransform( dummyTransform1 );

    PipelineHopMeta hi1 = new PipelineHopMeta( xsltTransform, dummyTransform1 );
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
    List<RowMetaAndData> inputList = createData( xslFilename );
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

    checkRows( goldenImageRows, resultRows, 2 );
  }

}
