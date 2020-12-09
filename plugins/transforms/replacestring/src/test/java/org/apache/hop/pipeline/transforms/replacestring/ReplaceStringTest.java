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

package org.apache.hop.pipeline.transforms.replacestring;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 1/31/14 Time: 11:19 AM
 */
public class ReplaceStringTest {

  private static final String LITERAL_STRING = "[a-z]{2,7}";

  private static final String INPUT_STRING = "This is String This Is String THIS IS STRING";

  private Object[] row = new Object[] { "some data", "another data" };

  private Object[] expectedRow = new Object[] { "some data", "1nother d1t1", "1no2her d121", null, null, null, null,
    null, null, null, null, null, null };

  private TransformMockHelper<ReplaceStringMeta, ReplaceStringData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<>( "REPLACE STRING TEST", ReplaceStringMeta.class,
        ReplaceStringData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString() );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString(), any( Object[].class ) );
    verify( transformMockHelper.iLogChannel, never() ).logError( anyString(), (Throwable) anyObject() );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testGetOneRow() throws Exception {
    ReplaceStringData data = new ReplaceStringData();
    ReplaceStringMeta meta = new ReplaceStringMeta();

    ReplaceString replaceString =
      new ReplaceString( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta( 0, new ValueMetaString( "SomeDataMeta" ) );
    inputRowMeta.addValueMeta( 1, new ValueMetaString( "AnotherDataMeta" ) );
    replaceString.init();
    replaceString.setInputRowMeta( inputRowMeta );
    data.outputRowMeta = inputRowMeta;
    data.outputRowMeta.addValueMeta( new ValueMetaString( "AnotherDataMeta" ) );
    data.inputFieldsNr = 2;
    data.numFields = 2;
    data.inStreamNrs = new int[] { 1, 1 };
    data.patterns = new Pattern[] { Pattern.compile( "a" ), Pattern.compile( "t" ) };
    data.replaceFieldIndex = new int[] { -1, -1 };
    data.outStreamNrs = new String[] { "", "1" };
    data.replaceByString = new String[] { "1", "2" };
    data.setEmptyString = new boolean[] { false, false };
    // when( inputRowMeta.size() ).thenReturn( 3 );
    // when( inputRowMeta.getString( anyObject(), 1 ) ).thenReturn((String) row[1]);

    Object[] output = replaceString.getOneRow( inputRowMeta, row );
    assertArrayEquals( "Output varies", expectedRow, output );
  }

  //PDI-16472
  @Test
  public void testSynchronizeDifferentFieldsArraysLengths() throws Exception {

    ReplaceStringData data = new ReplaceStringData();
    ReplaceStringMeta meta = new ReplaceStringMeta();
    ReplaceString replaceString =
      new ReplaceString( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );

    replaceString.init();

    meta.setFieldInStream( new String[] { "input1", "input2" } );
    meta.setFieldOutStream( new String[] { "out" } );
    meta.setUseRegEx( new int[] { 1 } );
    meta.setCaseSensitive( new int[] { 0 } );
    meta.setWholeWord( new int[] { 1 } );
    meta.setReplaceString( new String[] { "string" } );
    meta.setReplaceByString( new String[] { "string" } );
    meta.setEmptyString( new boolean[] { true } );
    meta.setFieldReplaceByString( new String[] { "string" } );

    meta.afterInjectionSynchronization();

    Assert.assertEquals( meta.getFieldInStream().length, meta.getFieldOutStream().length );
    Assert.assertEquals( StringUtils.EMPTY, meta.getFieldOutStream()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getUseRegEx().length );
    Assert.assertEquals( 0, meta.getUseRegEx()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getCaseSensitive().length );
    Assert.assertEquals( 0, meta.getCaseSensitive()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getWholeWord().length );
    Assert.assertEquals( 0, meta.getWholeWord()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getReplaceString().length );
    Assert.assertEquals( StringUtils.EMPTY, meta.getReplaceString()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getReplaceByString().length );
    Assert.assertEquals( StringUtils.EMPTY, meta.getReplaceByString()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.isSetEmptyString().length );
    Assert.assertEquals( false, meta.isSetEmptyString()[ 1 ] );

    Assert.assertEquals( meta.getFieldInStream().length, meta.getFieldReplaceByString().length );
    Assert.assertEquals( StringUtils.EMPTY, meta.getFieldReplaceByString()[ 1 ] );
  }

  @Test
  public void testBuildPatternWithLiteralParsingAndWholeWord() throws Exception {
    Pattern actualPattern = ReplaceString.buildPattern( true, true, true, LITERAL_STRING, false );
    Matcher matcher = actualPattern.matcher( INPUT_STRING );
    String actualString = matcher.replaceAll( "are" );
    Assert.assertEquals( INPUT_STRING, actualString );
  }

  @Test
  public void testBuildPatternWithNonLiteralParsingAndWholeWord() throws Exception {
    Pattern actualPattern = ReplaceString.buildPattern( false, true, true, LITERAL_STRING, false );
    Matcher matcher = actualPattern.matcher( INPUT_STRING );
    String actualString = matcher.replaceAll( "are" );
    Assert.assertEquals( "This are String This Is String THIS IS STRING", actualString );
  }

  @Test
  public void testProcessRow() throws Exception {
    ReplaceStringData data = new ReplaceStringData();
    ReplaceStringMeta meta = transformMockHelper.iTransformMeta;

    ReplaceString replaceString = Mockito.spy(
      new ReplaceString( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline ) );
    IRowMeta inputRowMeta = new RowMeta();
    byte[] array = { 0, 97, 0, 65, -1, 65, -1, 33 };
    byte[] matcharray = { -1, 33 };
    String match = new String( matcharray, "UTF-16BE" );
    Object[] Row = new Object[] { new String( array, "UTF-16BE" ), "another data" };
    doReturn( Row ).when( replaceString ).getRow();
    inputRowMeta.addValueMeta( 0, new ValueMetaString( "string" ) );

    doReturn( new String[] { "string" } ).when( meta ).getFieldInStream();
    doReturn( new String[] { "output" } ).when( meta ).getFieldOutStream();

    doReturn( new int[] { 1 } ).when( meta ).isUnicode();
    doReturn( new int[] { 0 } ).when( meta ).getUseRegEx();
    doReturn( new int[] { 0 } ).when( meta ).getCaseSensitive();
    doReturn( new int[] { 0 } ).when( meta ).getWholeWord();
    doReturn( new String[] { match } ).when( meta ).getReplaceString();
    doReturn( new String[] { "" } ).when( meta ).getFieldReplaceByString();
    doReturn( new String[] { "matched" } ).when( meta ).getReplaceByString();
    doReturn( new boolean[] { false } ).when( meta ).isSetEmptyString();

    replaceString.init();
    replaceString.setInputRowMeta( inputRowMeta );
    data.outputRowMeta = inputRowMeta;
    data.inputFieldsNr = 1;
    data.numFields = 1;
    data.inStreamNrs = new int[] { 0 };
    data.replaceFieldIndex = new int[] { -1 };
    data.outStreamNrs = new String[] { "", "1" };
    data.replaceByString = new String[] { "1" };
    data.setEmptyString = new boolean[] { false, false };

    replaceString.init();
    System.out.println( replaceString.getRow()[ 1 ] );
//    assertTrue( "Expected: aAmatchedmatched", "aAmatchedmatched".equals( replaceString.getRow()[ 1 ] ) );
  }
}
