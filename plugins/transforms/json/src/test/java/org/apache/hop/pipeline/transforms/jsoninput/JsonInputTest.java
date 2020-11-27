/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.jsoninput;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.InvalidPathException;
import junit.framework.ComparisonFailure;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.hop.core.util.Assert.assertNotNull;
import static org.apache.hop.core.util.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class JsonInputTest {

  protected static final String BASE_RAM_DIR = "ram:///jsonInputTest/";
  protected TransformMockHelper<JsonInputMeta, JsonInputData> helper;

  protected static final String getBasicTestJson() {
    return "{\n"
        + "  \"home\": {},\n"
        + "  \"store\": {\n"
        + "    \"book\": [\n"
        + "      {\n"
        + "        \"category\": \"reference\",\n"
        + "        \"author\": \"Nigel Rees\",\n"
        + "        \"title\": \"Sayings of the Century\",\n"
        + "        \"price\": 8.95\n"
        + "      },\n"
        + "      {\n"
        + "        \"category\": \"fiction\",\n"
        + "        \"author\": \"Evelyn Waugh\",\n"
        + "        \"title\": \"Sword of Honour\",\n"
        + "        \"price\": 12.99\n"
        + "      },\n"
        + "      {\n"
        + "        \"category\": \"fiction\",\n"
        + "        \"author\": \"Herman Melville\",\n"
        + "        \"title\": \"Moby Dick\",\n"
        + "        \"isbn\": \"0-553-21311-3\",\n"
        + "        \"price\": 8.99\n"
        + "      },\n"
        + "      {\n"
        + "        \"category\": \"fiction\",\n"
        + "        \"author\": \"J. R. R. Tolkien\",\n"
        + "        \"title\": \"The Lord of the Rings\",\n"
        + "        \"isbn\": \"0-395-19395-8\",\n"
        + "        \"price\": 22.99\n"
        + "      }\n"
        + "    ],\n"
        + "    \"bicycle\": {\n"
        + "      \"color\": \"red\",\n"
        + "      \"price\": 19.95\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  private static final String getPDI17060Json() {
    return "{"
        + " \"path\": \"/board/offer-sources/phases/current/cards/acquisitions\","
        + " \"id\": \"acquisitions\","
        + " \"template\": \"offer-sources\","
        + " \"creator\": \"admin\","
        + " \"created\": 1491703768197,"
        + " \"modifiedby\": null,"
        + " \"modified\": null,"
        + " \"color\": \"blue\","
        + " \"fields\": {"
        + "   \"group-detail\": \"Offer Source Details\","
        + "   \"name\": \"Acquisitions\""
        + " },"
        + " \"tasks\": 0,"
        + " \"history\": 1,"
        + " \"attachments\": 0,"
        + " \"comments\": 0,"
        + " \"alerts\": 0,"
        + " \"title\": \"Acquisitions\","
        + " \"lock\": null,"
        + " \"completeTasks\": null,"
        + " \"phase\": \"current\","
        + " \"errors\": null,"
        + " \"board\": \"offer-sources\""
        + "}";
  }

  @BeforeClass
  public static void init() throws HopException {
    HopClientEnvironment.init();
  }

  @Before
  public void setUp() {
    helper =
      new TransformMockHelper<JsonInputMeta, JsonInputData>( "json input test", JsonInputMeta.class, JsonInputData.class );
    when( helper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      helper.iLogChannel );
    when( helper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    helper.cleanUp();
  }

  @Test
  public void testAttrFilter() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[?(@.isbn)].author", new ValueMetaString( "author w/ isbn" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "Herman Melville" },
        new Object[] { jsonInputField, "J. R. R. Tolkien" } } );
  }

  @Test
  public void testChildDot() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.store.bicycle.color", new ValueMetaString( "bcol" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "red" } } );
    testSimpleJsonPath( "$.store.bicycle.price", new ValueMetaNumber( "p" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, 19.95 } } );
  }

  @Test
  public void testChildBrackets() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.['store']['bicycle']['color']", new ValueMetaString( "bcol" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "red" } } );
  }

  @Test
  public void testChildBracketsNDots() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.['store'].['bicycle'].['color']", new ValueMetaString( "bcol" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "red" } } );
  }

  @Test
  public void testIndex() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[2].title", new ValueMetaString( "title" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "Moby Dick" } } );
  }

  @Test
  public void testIndexFirst() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[:2].category", new ValueMetaString( "category" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, "reference" },
        new Object[] { jsonInputField, "fiction" } } );
  }

  @Test
  public void testIndexLastObj() throws Exception {
    final String jsonInputField = getBasicTestJson();
    JsonInput jsonInput =
      createBasicTestJsonInput( "$..book[-1:]", new ValueMetaString( "last book" ), "json",
        new Object[] { jsonInputField } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { jsonInputField,
        "{ \"category\": \"fiction\",\n"
          + "  \"author\": \"J. R. R. Tolkien\",\n"
          + "  \"title\": \"The Lord of the Rings\",\n"
          + "  \"isbn\": \"0-395-19395-8\",\n"
          + "  \"price\": 22.99\n"
          + "}\n" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testIndexList() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[1,3].price", new ValueMetaNumber( "price" ),
      new Object[][] { new Object[] { jsonInputField } },
      new Object[][] { new Object[] { jsonInputField, 12.99 },
        new Object[] { jsonInputField, 22.99 } } );
  }

  @Test
  public void testSingleField() throws Exception {
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", isbn );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { null, "0-553-21311-3" },
      new Object[] { null, "0-395-19395-8" } );
    rowComparator.setComparator( 0, null );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "error", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDualExp() throws Exception {
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( IValueMeta.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[?(@.isbn)].price" );
    price.setType( IValueMeta.TYPE_NUMBER );

    JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { null, "0-553-21311-3", 8.99 },
      new Object[] { null, "0-395-19395-8", 22.99 } );
    rowComparator.setComparator( 0, null );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "error", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDualExpMismatchError() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( IValueMeta.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[*].price" );
    price.setType( IValueMeta.TYPE_NUMBER );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
      JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );

      processRows( jsonInput, 3 );

      Assert.assertEquals( "error", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      String expectedError =
        "The data structure is not the same inside the resource!"
          + " We found 4 values for json path [$..book[*].price],"
          + " which is different that the number returned for path [$..book[?(@.isbn)].isbn] (2 values)."
          + " We MUST have the same number of values for all paths.";
      Assert.assertTrue( "expected error", errors.contains( expectedError ) );
    }
  }

  @Test
  public void testBadExp() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {

      ValueMetaString outputMeta = new ValueMetaString( "result" );

      JsonInputField jpath = new JsonInputField( outputMeta.getName() );
      jpath.setPath( "$..fail" );
      jpath.setType( outputMeta.getType() );

      JsonInputMeta jsonInputMeta = createSimpleMeta( "json", jpath );
      jsonInputMeta.setIgnoreMissingPath( false );

      JsonInput jsonInput = createJsonInput( "json", jsonInputMeta, new Object[] { getBasicTestJson() } );

      processRows( jsonInput, 2 );
      Assert.assertEquals( "errors", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );

      String expectedError = "We can not find any data with path [$..fail]";
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      Assert.assertTrue( "error", errors.contains( expectedError ) );
    }
  }

  @Test
  public void testRemoveSourceField() throws Exception {
    final String inCol = "json";
    JsonInputField jpath = new JsonInputField( "isbn" );
    jpath.setPath( "$..book[*].isbn" );
    jpath.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( inCol, jpath );
    meta.setRemoveSourceField( true );
    meta.setIgnoreMissingPath( true );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { "0-553-21311-3" },
      new Object[] { "0-395-19395-8" } );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 4 );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testRowLimit() throws Exception {
    final String inCol = "json";
    JsonInputField jpath = new JsonInputField( "isbn" );
    jpath.setPath( "$..book[*].isbn" );
    jpath.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( inCol, jpath );
    meta.setRemoveSourceField( true );
    meta.setIgnoreMissingPath( true );
    meta.setRowLimit( 2 );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    processRows( jsonInput, 4 );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testSmallDoubles() throws Exception {
    // legacy parser handles these but positive exp would read null
    for ( String nbr : new String[] { "1e-20", "1.52999996e-20", "2.05E-20" } ) {
      final String ibgNbrInput =
        "{ \"number\": " + nbr + " }";
      testSimpleJsonPath( "$.number", new ValueMetaNumber( "not so big number" ),
        new Object[][] { new Object[] { ibgNbrInput } },
        new Object[][] { new Object[] { ibgNbrInput, Double.parseDouble( nbr ) } } );
    }
  }

  @Test
  public void testJgdArray() throws Exception {
    final String input =
      " { \"arr\": [ [ { \"a\": 1, \"b\": 1}, { \"a\": 1, \"b\": 2} ], [ {\"a\": 3, \"b\": 4 } ] ] }";
    JsonInput jsonInput =
      createBasicTestJsonInput( "$.arr", new ValueMetaString( "array" ), "json", new Object[] { input } );
    RowComparatorListener rowComparator =
      new RowComparatorListener(
        new Object[] { input, "[[{\"a\":1,\"b\":1},{\"a\":1,\"b\":2}],[{\"a\":3,\"b\":4}]]" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDefaultLeafToNull() throws Exception {
    JsonInputField noPath = new JsonInputField( "price" );
    noPath.setPath( "$..price" );
    noPath.setType( IValueMeta.TYPE_STRING );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputMeta meta = createSimpleMeta( "json", noPath );
    meta.setIgnoreMissingPath( true );
    meta.setRemoveSourceField( true );
    final String input = getBasicTestJson();

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
    processRows( jsonInput, 8 );
    disposeJsonInput( jsonInput );

    Assert.assertEquals( 5, jsonInput.getLinesWritten() );
  }

  // There are tests for PDI-17060 below
  @Test
  public void testDefaultLeafToNullChangedToFalse_NoNullInOutput() throws Exception {
    JsonInputField id = new JsonInputField( "id" );
    id.setPath( "$..id" );
    id.setType( IValueMeta.TYPE_STRING );
    JsonInputField name = new JsonInputField( "name" );
    name.setPath( "$..name" );
    name.setType( IValueMeta.TYPE_STRING );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputMeta meta = createSimpleMeta( "json", id, name );
    // For these user who wanted to have "old" behavior
    meta.setDefaultPathLeafToNull( false );
    meta.setIgnoreMissingPath( true );
    final String input = getPDI17060Json();

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
    jsonInput.addRowListener( new RowComparatorListener( new Object[] { input, "acquisitions", "Acquisitions" } ) );
    processRows( jsonInput, 8 );
    disposeJsonInput( jsonInput );

    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDefaultLeafToNullTrue_NullsInOutput() throws Exception {
    JsonInputField id = new JsonInputField( "id" );
    id.setPath( "$..id" );
    id.setType( IValueMeta.TYPE_STRING );
    JsonInputField name = new JsonInputField( "name" );
    name.setPath( "$..name" );
    name.setType( IValueMeta.TYPE_STRING );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputMeta meta = createSimpleMeta( "json", id, name );
    meta.setIgnoreMissingPath( true );
    final String input = getPDI17060Json();

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
    jsonInput.addRowListener( new RowComparatorListener( new Object[] { input, "acquisitions", null }, new Object[] { input, null, "Acquisitions" } ) );
    processRows( jsonInput, 8 );
    disposeJsonInput( jsonInput );

    Assert.assertEquals( 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testIfIgnorePathDoNotSkipRowIfInputIsNullOrFieldNotFound() throws Exception {

    final String input1 = "{ \"value1\": \"1\",\n"
            + "  \"value2\": \"2\",\n"
            + "}";
    final String input2 = "{ \"value1\": \"3\""
            + "}";
    final String input3 = "{ \"value2\": \"4\""
            + "}";
    final String input4 = "{ \"value1\": null,\n"
            + "  \"value2\": null,\n"
            + "}";
    final String input5 = "{}";
    final String input6 = null;

    final String inCol = "input";

    JsonInputField aField = new JsonInputField();
    aField.setName( "a" );
    aField.setPath( "$.value1" );
    aField.setType( IValueMeta.TYPE_STRING );
    JsonInputField bField = new JsonInputField();
    bField.setName( "b" );
    bField.setPath( "$.value2" );
    bField.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( inCol, aField, bField );
    meta.setIgnoreMissingPath( true );
    JsonInput transform = createJsonInput( inCol, meta, new Object[] { input1 },
             new Object[] { input2 },
             new Object[] { input3 },
             new Object[] { input4 },
             new Object[] { input5 },
             new Object[] { input6 }
            );
    transform.addRowListener(
      new RowComparatorListener(
        new Object[]{ input1, "1", "2" }, new Object[]{ input2, "3", null }, new Object[]{ input3, null, "4" },
        new Object[]{ input4, null, null }, new Object[]{ input5, null, null }, new Object[]{ input6, null, null } ) );
    processRows( transform, 5 );
  }

  @Test
  public void testBfsMatchOrder() throws Exception {
    // streaming will be dfs..ref impl is bfs
    String input = "{ \"a\": { \"a\" : { \"b\" :2 } , \"b\":1 } }";
    JsonInput jsonInput =
      createBasicTestJsonInput( "$..a.b", new ValueMetaInteger( "b" ), "in", new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener( jsonInput,
      new Object[] { input, 1L },
      new Object[] { input, 2L } );
    rowComparator.setComparator( 0, null );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testRepeatFieldSingleObj() throws Exception {
    final String input = " { \"items\": [ "
      + "{ \"a\": 1, \"b\": null }, "
      + "{ \"a\":null, \"b\":2 }, "
      + "{ \"a\":3, \"b\":null }, "
      + "{ \"a\":4, \"b\":4 } ] }";
    final String inCol = "input";

    JsonInputField aField = new JsonInputField();
    aField.setName( "a" );
    aField.setPath( "$.items[*].a" );
    aField.setType( IValueMeta.TYPE_INTEGER );
    JsonInputField bField = new JsonInputField();
    bField.setName( "b" );
    bField.setPath( "$.items[*].b" );
    bField.setType( IValueMeta.TYPE_INTEGER );
    bField.setRepeated( true );

    JsonInputMeta meta = createSimpleMeta( inCol, aField, bField );
    meta.setIgnoreMissingPath( true );
    JsonInput step = createJsonInput( inCol, meta, new Object[] { input } );
    step.addRowListener(
      new RowComparatorListener(
        new Object[] { input, 1L, null },
        new Object[] { input, null, 2L },
        new Object[] { input, 3L, 2L },
        new Object[] { input, 4L, 4L } ) );
    processRows( step, 4 );
    Assert.assertEquals( 4, step.getLinesWritten() );
  }

  @Test
  public void testPathMissingIgnore() throws Exception {
    final String input = "{ \"value1\": \"1\",\n"
      + "  \"value2\": \"2\",\n"
      + "}";
    final String inCol = "input";

    JsonInputField aField = new JsonInputField();
    aField.setName( "a" );
    aField.setPath( "$.value1" );
    aField.setType( IValueMeta.TYPE_STRING );
    JsonInputField bField = new JsonInputField();
    bField.setName( "b" );
    bField.setPath( "$.value2" );
    bField.setType( IValueMeta.TYPE_STRING );
    JsonInputField cField = new JsonInputField();
    cField.setName( "c" );
    cField.setPath( "$.notexistpath.value3" );
    cField.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( inCol, aField, bField, cField );
    meta.setIgnoreMissingPath( true );
    JsonInput step = createJsonInput( inCol, meta, new Object[] { input } );
    step.addRowListener(
      new RowComparatorListener(
        new Object[] { input, "1", "2", null } ) );
    processRows( step, 1 );
    Assert.assertEquals( 1, step.getLinesWritten() );
  }

  /**
   * PDI-10384 Huge numbers causing exception in JSON input step<br>
   */
  @Test
  public void testLargeDoubles() throws Exception {
    // legacy mode yields null for these
    for ( String nbr : new String[] { "1e20", "2.05E20", "1.52999996e20" } ) {
      final String ibgNbrInput =
        "{ \"number\": " + nbr + " }";
      testSimpleJsonPath( "$.number", new ValueMetaNumber( "not so big number" ),
        new Object[][] { new Object[] { ibgNbrInput } },
        new Object[][] { new Object[] { ibgNbrInput, Double.parseDouble( nbr ) } } );
    }
  }


  @Test
  public void testNullProp() throws Exception {
    final String input = "{ \"obj\": [ { \"nval\": null, \"val\": 2 }, { \"val\": 1 } ] }";
    JsonInput jsonInput =
      createBasicTestJsonInput( "$.obj[?(@.nval)].val", new ValueMetaString( "obj" ), "json", new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { input, "2" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    // in jsonpath 2.0->2.1, null value properties started being counted as existing
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDualExpMismatchPathLeafToNull() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[*].isbn" );
    isbn.setType( IValueMeta.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[*].price" );
    price.setType( IValueMeta.TYPE_NUMBER );

    JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
    meta.setIgnoreMissingPath( true );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { null, 8.95d },
      new Object[] { null, 12.99d },
      new Object[] { "0-553-21311-3", 8.99d },
      new Object[] { "0-395-19395-8", 22.99d } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 5 );

    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 4, jsonInput.getLinesWritten() );
  }

  @Test
  public void testSingleObjPred() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField bic = new JsonInputField( "color" );
    bic.setPath( "$.store.bicycle[?(@.price)].color" );
    bic.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", bic );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { "red" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testArrayOut() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField byc = new JsonInputField( "books (array)" );
    byc.setPath( "$.store.book" );
    byc.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", byc );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] {
        "[{\"category\":\"reference\",\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\",\"price\":8.95},"
          + "{\"category\":\"fiction\",\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"price\":12.99},"
          + "{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\","
          + "\"isbn\":\"0-553-21311-3\",\"price\":8.99},{\"category\":\"fiction\",\"author\":\"J. R. R. Tolkien\","
          + "\"title\":\"The Lord of the Rings\",\"isbn\":\"0-395-19395-8\",\"price\":22.99}]" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testObjectOut() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField bic = new JsonInputField( "the bicycle (obj)" );
    bic.setPath( "$.store.bicycle" );
    bic.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", bic );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { "{\"color\":\"red\",\"price\":19.95}" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testBicycleAsterisk() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField byc = new JsonInputField( "badger" );
    byc.setPath( "$.store.bicycle[*]" );
    byc.setType( IValueMeta.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", byc );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
      new Object[] { "red" },
      new Object[] { "19.95" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    assertEquals( out.toString(), 0, jsonInput.getErrors() );
    assertEquals( "rows written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testNullInputs() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[?(@.isbn)].author", new ValueMetaString( "author w/ isbn" ),
      new Object[][] {
        new Object[] { null },
        new Object[] { jsonInputField },
        new Object[] { null } },
      new Object[][] {
        new Object[] { null, null },
        new Object[] { jsonInputField, "Herman Melville" },
        new Object[] { jsonInputField, "J. R. R. Tolkien" },
        new Object[] { null, null }
      } );
  }

  /**
   * File tests
   */
  @Test
  public void testNullFileList() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    try {
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( IValueMeta.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );
      List<FileObject> fileList = Arrays.asList( null, null );
      JsonInputMeta meta = createFileListMeta( fileList );
      meta.setInputFields( new JsonInputField[] { price } );

      meta.setIncludeRowNumber( true );
      meta.setRowNumberField( "rownbr" );
      meta.setShortFileNameField( "fname" );

      JsonInput jsonInput = createJsonInput( meta );
      processRows( jsonInput, 5 );
      disposeJsonInput( jsonInput );
      assertEquals( err.toString(), 2, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }

  @Test
  public void testFileList() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input1 = getBasicTestJson();
    final String input2 = "{ \"store\": { \"book\": [ { \"price\": 9.99 } ] } }";
    try ( FileObject fileObj1 = HopVfs.getFileObject( BASE_RAM_DIR + "test1.json" );
          FileObject fileObj2 = HopVfs.getFileObject( BASE_RAM_DIR + "test2.json" ) ) {
      try ( OutputStream out = fileObj1.getContent().getOutputStream() ) {
        out.write( input1.getBytes() );
      }
      try ( OutputStream out = fileObj2.getContent().getOutputStream() ) {
        out.write( input2.getBytes() );
      }
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( IValueMeta.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );
      List<FileObject> fileList = Arrays.asList( fileObj1, fileObj2 );
      JsonInputMeta meta = createFileListMeta( fileList );
      meta.setInputFields( new JsonInputField[] { price } );

      meta.setIncludeRowNumber( true );
      meta.setRowNumberField( "rownbr" );

      meta.setShortFileNameField( "fname" );

      JsonInput jsonInput = createJsonInput( meta );
      RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { 8.95d, 1L, "test1.json" },
        new Object[] { 12.99d, 2L, "test1.json" },
        new Object[] { 8.99d, 3L, "test1.json" },
        new Object[] { 22.99d, 4L, "test1.json" },
        new Object[] { 9.99d, 5L, "test2.json" } );
      jsonInput.addRowListener( rowComparator );

      processRows( jsonInput, 5 );
      disposeJsonInput( jsonInput );
      assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }


  @Test
  public void testNoFilesInListError() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    JsonInputMeta meta = createFileListMeta( Collections.<FileObject>emptyList() );
    meta.setDoNotFailIfNoFile( false );
    JsonInputField price = new JsonInputField();
    price.setName( "price" );
    price.setType( IValueMeta.TYPE_NUMBER );
    price.setPath( "$..book[*].price" );
    meta.setInputFields( new JsonInputField[] { price } );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInput jsonInput = createJsonInput( meta );
      processRows( jsonInput, 1 );
    }
    String errMsgs = err.toString();
    assertTrue( errMsgs, errMsgs.contains( "No file(s) specified!" ) );
  }

  @Test
  public void testZipFileInput() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input = getBasicTestJson();
    try ( FileObject fileObj = HopVfs.getFileObject( BASE_RAM_DIR + "test.zip" ) ) {
      fileObj.createFile();
      try ( OutputStream out = fileObj.getContent().getOutputStream() ) {
        try ( ZipOutputStream zipOut = new ZipOutputStream( out ) ) {
          ZipEntry jsonFile = new ZipEntry( "test.json" );
          zipOut.putNextEntry( jsonFile );
          zipOut.write( input.getBytes("UTF-8") );
          zipOut.closeEntry();
          zipOut.flush();
        }
      }

      String json = FileObjectUtils.getContentAsString( HopVfs.getFileObject( "zip:" + BASE_RAM_DIR + "test.zip!/test.json" ), "UTF-8" );

      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( IValueMeta.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );

      JsonInputMeta meta = createSimpleMeta( "in file", price );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { "zip:" + BASE_RAM_DIR + "test.zip!/test.json" }
      } );
      RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { 8.95d },
        new Object[] { 12.99d },
        new Object[] { 8.99d },
        new Object[] { 22.99d } );
      jsonInput.addRowListener( rowComparator );
      processRows( jsonInput, 5 );
      Assert.assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }

  @Test
  public void testExtraFileFields() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input1 = getBasicTestJson();
    final String input2 = "{ \"store\": { \"bicycle\": { \"color\": \"blue\" } } }";
    final String path1 = BASE_RAM_DIR + "test1.json";
    final String path2 = BASE_RAM_DIR + "test2.js";
    try ( FileObject fileObj1 = HopVfs.getFileObject( path1 );
          FileObject fileObj2 = HopVfs.getFileObject( path2 ) ) {
      try ( OutputStream out = fileObj1.getContent().getOutputStream() ) {
        out.write( input1.getBytes() );
      }
      try ( OutputStream out = fileObj2.getContent().getOutputStream() ) {
        out.write( input2.getBytes() );
      }

      JsonInputField color = new JsonInputField();
      color.setName( "color" );
      color.setType( IValueMeta.TYPE_STRING );
      color.setPath( "$.store.bicycle.color" );

      JsonInputMeta meta = createSimpleMeta( "in file", color );
      meta.setInFields( true );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );

      meta.setExtensionField( "extension" );
      meta.setPathField( "dir path" );
      meta.setSizeField( "size" );
      meta.setIsHiddenField( "hidden?" );
      meta.setLastModificationDateField( "last modified" );
      meta.setUriField( "URI" );
      meta.setRootUriField( "root URI" );

      // custom checkers for size and last modified
      RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "red",
          "json", "ram:///jsonInputTest", -1L, false, new Date( 0 ), "ram:///jsonInputTest/test1.json", "ram:///" },
        new Object[] { "blue",
          "js", "ram:///jsonInputTest", -1L, false, new Date( 0 ), "ram:///jsonInputTest/test2.js", "ram:///" } );
      rowComparator.setComparator( 3, ( expected, actual ) -> {
        // just want a valid size
        return ( (long) actual ) > 0L;
      } );
      rowComparator.setComparator( 5, ( expected, actual ) -> ( (Date) actual ).after( new Date( 0 ) ) );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { path1 },
        new Object[] { path2 }
      } );
      jsonInput.addRowListener( rowComparator );
      processRows( jsonInput, 3 );
      Assert.assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }

  @Test
  public void testZeroSizeFile() throws Exception {
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    helper.redirectLog( log, LogLevel.BASIC );
    try ( FileObject fileObj = HopVfs.getFileObject( BASE_RAM_DIR + "test.json" );
          LocaleChange enUs = new LocaleChange( Locale.US ); ) {
      fileObj.createFile();
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( IValueMeta.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );

      JsonInputMeta meta = createSimpleMeta( "in file", price );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );
      meta.setIgnoreEmptyFile( false );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { BASE_RAM_DIR + "test.json" }
      } );
      processRows( jsonInput, 1 );
      String logMsgs = log.toString();
      assertTrue( logMsgs, logMsgs.contains( "is empty!" ) );
    } finally {
      deleteFiles();
    }
  }

  /**
   * PDI-13859
   */
  @Test
  public void testBracketEscape() throws Exception {
    String input = "{\"a\":1,\"b(1)\":2}";
    testSimpleJsonPath( "$.['b(1)']", new ValueMetaInteger( "b(1)" ),
      new Object[][] { new Object[] { input } },
      new Object[][] { new Object[] { input, 2L } } );
  }


  @Test
  public void testBadInput() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( IValueMeta.TYPE_STRING );

    String input = "{{";
    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInputMeta meta = createSimpleMeta( "json", isbn );
      JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
      processRows( jsonInput, 3 );

      Assert.assertEquals( "error", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      Assert.assertTrue( "expected error", errors.contains( "Error parsing string" ) );
    }
  }

  @Test
  public void testErrorRedirect() throws Exception {
    JsonInputField field = new JsonInputField( "value" );
    field.setPath( "$.value" );
    field.setType( IValueMeta.TYPE_STRING );

    String input1 = "{{";
    String input2 = "{ \"value\": \"ok\" }";

    JsonInputMeta meta = createSimpleMeta( "json", field );
    meta.setRemoveSourceField( true );
    when( helper.transformMeta.isDoingErrorHandling() ).thenReturn( true );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input1 }, new Object[] { input2 } );
    TransformErrorMeta errMeta = new TransformErrorMeta( jsonInput, helper.transformMeta );
    errMeta.setEnabled( true );
    errMeta.setErrorFieldsValuename( "err field" );
    when( helper.transformMeta.getTransformErrorMeta() ).thenReturn( errMeta );
    final List<Object[]> errorLines = new ArrayList<>();
    jsonInput.addRowListener( new RowComparatorListener( new Object[] { "ok" } ) {
      @Override
      public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        errorLines.add( row );
      }
    } );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "fwd error", 1, errorLines.size() );
    Assert.assertEquals( "input in err line", input1, errorLines.get( 0 )[ 0 ] );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testUrlInput() throws Exception {
    JsonInputField field = new JsonInputField( "value" );
    field.setPath( "$.value" );
    field.setType( IValueMeta.TYPE_STRING );

    String input1 = "http://localhost/test.json";

    JsonInputMeta meta = createSimpleMeta( "json", field );
    meta.setReadUrl( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input1 } );
    processRows( jsonInput, 3 );

    Assert.assertEquals( 1, jsonInput.getErrors() );
  }

  @Test
  public void testJsonInputPathResolutionSuccess() {
    JsonInputField inputField = new JsonInputField( "value" );
    final String PATH = "${PARAM_PATH}.price";
    inputField.setPath( PATH );
    inputField.setType( IValueMeta.TYPE_STRING );
    final JsonInputMeta inputMeta = createSimpleMeta( "json", inputField );
    IVariables variables = new Variables();
    JsonInput jsonInput = null;
    try {
      jsonInput =
        createJsonInput( "json", inputMeta, variables, new Object[] { getBasicTestJson() } );
      fail( "Without the parameter, this call should fail with an InvalidPathException. If it does not, test fails." );
    } catch ( InvalidPathException pathException ) {
      assertNull( jsonInput );
    }

    variables.setVariable( "PARAM_PATH", "$..book.[*]" );

    try {
      jsonInput = createJsonInput( "json", inputMeta, variables, new Object[] { getBasicTestJson() } );
      assertNotNull( jsonInput );
    } catch ( Exception ex ) {
      fail( "Json Input should be able to resolve the paths with the parameter introduced in the variable space." );
    }
  }

  protected JsonInputMeta createSimpleMeta( String inputColumn, JsonInputField... jsonPathFields ) {
    JsonInputMeta jsonInputMeta = new JsonInputMeta();
    jsonInputMeta.setDefault();
    jsonInputMeta.setInFields( true );
    jsonInputMeta.setFieldValue( inputColumn );
    jsonInputMeta.setInputFields( jsonPathFields );
    jsonInputMeta.setIgnoreMissingPath( true );
    return jsonInputMeta;
  }

  private void deleteFiles() throws FileSystemException, HopFileException {
    try ( FileObject baseDir = HopVfs.getFileObject( BASE_RAM_DIR ) ) {
      baseDir.deleteAll();
    }
  }

  protected JsonInput createJsonInput( JsonInputMeta meta ) {
    JsonInputData data = new JsonInputData();

    JsonInput jsonInput = new JsonInput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline );
    jsonInput.init();
    return jsonInput;
  }

  protected void disposeJsonInput( JsonInput jsonInput ) {
    jsonInput.dispose();
  }

  protected JsonInputMeta createFileListMeta( final List<FileObject> files ) {
    JsonInputMeta meta = new JsonInputMeta() {
      @Override
      public FileInputList getFileInputList( IVariables space ) {
        return new FileInputList() {
          @Override
          public List<FileObject> getFiles() {
            return files;
          }

          @Override
          public int nrOfFiles() {
            return files.size();
          }
        };
      }
    };
    meta.setDefault();
    meta.setInFields( false );
    meta.setIgnoreMissingPath( false );
    return meta;
  }

  protected void testSimpleJsonPath( String jsonPath,
                                     IValueMeta outputMeta,
                                     Object[][] inputRows, Object[][] outputRows ) throws Exception {
    final String inCol = "in";

    JsonInput jsonInput = createBasicTestJsonInput( jsonPath, outputMeta, inCol, inputRows );

    RowComparatorListener rowComparator = new RowComparatorListener( outputRows );

    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, outputRows.length + 1 );
    Assert.assertEquals( "rows written", outputRows.length, jsonInput.getLinesWritten() );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
  }

  protected void processRows( ITransform transform, final int maxCalls ) throws Exception {
    for ( int outRowIdx = 0; outRowIdx < maxCalls; outRowIdx++ ) {
      if ( !transform.processRow() ) {
        break;
      }
    }
  }

  protected JsonInput createBasicTestJsonInput( String jsonPath, IValueMeta outputMeta, final String inCol,
                                                Object[]... inputRows ) {
    JsonInputField jpath = new JsonInputField( outputMeta.getName() );
    jpath.setPath( jsonPath );
    jpath.setType( outputMeta.getType() );

    JsonInputMeta meta = createSimpleMeta( inCol, jpath );
    return createJsonInput( inCol, meta, inputRows );
  }

  protected JsonInput createJsonInput( final String inCol, JsonInputMeta meta, Object[]... inputRows ) {
    return createJsonInput( inCol, meta, null, inputRows );
  }

  protected JsonInput createJsonInput( final String inCol, JsonInputMeta meta, IVariables variables, Object[]... inputRows ) {
    JsonInputData data = new JsonInputData();

    IRowSet input = helper.getMockInputRowSet( inputRows );
    IRowMeta rowMeta = createRowMeta( new ValueMetaString( inCol ) );
    input.setRowMeta( rowMeta );

    JsonInput jsonInput = new JsonInput( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline );

    jsonInput.addRowSetToInputRowSets( input );
    jsonInput.setInputRowMeta( rowMeta );
    jsonInput.initializeVariablesFrom( variables );

    jsonInput.init();
    return jsonInput;
  }

  protected static class RowComparatorListener extends RowAdapter {

    Object[][] data;
    int rowNbr = 0;
    private Map<Integer, Comparison<Object>> comparators = new HashMap<>();

    public RowComparatorListener( Object[]... data ) {
      this.data = data;
    }

    public RowComparatorListener( ITransform step, Object[]... data ) {
      this.data = data;
      step.addRowListener( this );
    }

    /**
     * @param colIdx
     * @param comparator
     */
    public void setComparator( int colIdx, Comparison<Object> comparator ) {
      comparators.put( colIdx, comparator );
    }

    @Override
    public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      if ( rowNbr >= data.length ) {
        throw new ComparisonFailure( "too many output rows", "" + data.length, "" + ( rowNbr + 1 ) );
      } else {
        for ( int i = 0; i < data[ rowNbr ].length; i++ ) {
          try {
            boolean eq = true;
            if ( comparators.containsKey( i ) ) {
              Comparison<Object> comp = comparators.get( i );
              if ( comp != null ) {
                eq = comp.equals( data[ rowNbr ][ i ], row[ i ] );
              }
            } else {
              IValueMeta valueMeta = rowMeta.getValueMeta( i );
              eq = valueMeta.compare( data[ rowNbr ][ i ], row[ i ] ) == 0;
            }
            if ( !eq ) {
              throw new ComparisonFailure( String.format( "Mismatch row %d, column %d", rowNbr, i ),
                rowMeta.getString( data[ rowNbr ] ), rowMeta.getString( row ) );
            }
          } catch ( Exception e ) {
            throw new AssertionError( String.format( "Value type at row %d, column %d", rowNbr, i ), e );
          }
        }
        rowNbr++;
      }
    }

    protected static interface Comparison<T> {
      public boolean equals( T expected, T actual ) throws Exception;
    }
  }

  protected static class JsonComparison implements RowComparatorListener.Comparison<Object> {
    @Override
    public boolean equals( Object expected, Object actual ) throws Exception {
      return jsonEquals( (String) expected, (String) actual );
    }
  }

  protected static class LocaleChange implements AutoCloseable {

    private Locale original;

    public LocaleChange( Locale newLocale ) {
      original = LanguageChoice.getInstance().getDefaultLocale();
      LanguageChoice.getInstance().setDefaultLocale( newLocale );
    }

    @Override
    public void close() throws Exception {
      LanguageChoice.getInstance().setDefaultLocale( original );
    }
  }

  /**
   * compare json (deep equals ignoring order)
   */
  protected static final boolean jsonEquals( String json1, String json2 ) throws Exception {
    ObjectMapper om = new ObjectMapper();
    JsonNode parsedJson1 = om.readTree( json1 );
    JsonNode parsedJson2 = om.readTree( json2 );
    return parsedJson1.equals( parsedJson2 );
  }

  protected static IRowMeta createRowMeta( IValueMeta ... valueMetas ) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList( Arrays.asList( valueMetas ) );
    return rowMeta;
  }

  @Test
  public void testJsonInputMetaInputFieldsNotOverwritten() throws Exception {
    JsonInputField inputField = new JsonInputField();
    final String PATH = "$..book[?(@.category=='${category}')].price";
    inputField.setPath( PATH );
    inputField.setType( IValueMeta.TYPE_STRING );
    final JsonInputMeta inputMeta = createSimpleMeta( "json", inputField );
    IVariables variables = new Variables();
    variables.setVariable( "category", "fiction" );
    JsonInput jsonInput = createJsonInput( "json", inputMeta, variables, new Object[] { getBasicTestJson() } );
    processRows( jsonInput, 2 );
    assertEquals( "Meta input fields paths should be the same after processRows", PATH, inputMeta.getInputFields()[0].getPath() );
  }

}
