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

package org.apache.hop.pipeline.transforms.mongodboutput;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import junit.framework.Assert;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.pipeline.transforms.mongodbinput.BaseMongoDbTransformTest;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputData.MongoTopLevel;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputData.hopRowToMongo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for MongoDbOutput */
public class MongoDbOutputTest extends BaseMongoDbTransformTest {
  private static final Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  @Mock private MongoDbOutputData iTransformData;
  @Mock private MongoDbOutputMeta iTransformMeta;

  private MongoDbOutput dbOutput;
  RowMeta rowMeta = new RowMeta();
  Object[] rowData;
  List<Object[]> outputRowData;

  private List<MongoDbOutputMeta.MongoField> mongoFields =
    new ArrayList<>();

  @Before
  public void before() throws MongoDbException {
    super.before();

    when(iTransformData.getConnection()).thenReturn(mongoClientWrapper);
    when(iTransformMeta.getPort()).thenReturn("1010");
    when(iTransformMeta.getHostnames()).thenReturn("host");

    dbOutput =
        new MongoDbOutput(
            transformMeta, iTransformMeta, iTransformData, 1, pipelineMeta, pipeline) {
          public Object[] getRow() {
            return rowData;
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            outputRowData.add(row);
          }

          public IRowMeta getInputRowMeta() {
            return rowMeta;
          }
        };
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.init();
  }

  @Test
  public void testCheckTopLevelConsistencyPathsAreConsistentRecord() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, ""), mf("field2", true, ""));

    MongoTopLevel topLevel = MongoDbOutputData.checkTopLevelConsistency(paths, new Variables());
    assertTrue(topLevel == MongoTopLevel.RECORD);
  }

  @Test
  public void testCheckTopLevelConsistencyPathsAreConsistentArray() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, "[0]"), mf("field2", true, "[0]"));

    MongoTopLevel topLevel = MongoDbOutputData.checkTopLevelConsistency(paths, new Variables());
    assertTrue(topLevel == MongoTopLevel.ARRAY);
  }

  @Test
  public void testCheckTopLevelConsistencyPathsAreInconsistent() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, ""), mf("field2", true, "[0]"));

    MongoTopLevel topLevel = MongoDbOutputData.checkTopLevelConsistency(paths, new Variables());
    assertTrue(topLevel == MongoTopLevel.INCONSISTENT);
  }

  @Test(expected = HopException.class)
  public void testCheckTopLevelConsistencyNoFieldsDefined() throws Exception {
    MongoDbOutputData.checkTopLevelConsistency(
      new ArrayList<>( 0 ), new Variables());
  }

  /**
   * PDI-11045. When doing a non-modifier update/upsert it should not be necessary to define query
   * fields a second time in the transform (i.e. those paths that the user defines for the matching
   * conditions should be placed into the update document automatically).
   *
   * @throws HopException
   */
  @Test
  public void testUpdateObjectContainsQueryFields() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    mf.m_updateMatchField = true;
    paths.add(mf);

    mf = mf("field2", true, "");
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    assertEquals(JSON.serialize(result), "{ \"field1\" : \"value1\" , \"field2\" : 12}");
  }

  /**
   * PDI-11045. Here we test backwards compatibility for old ktrs that were developed before 11045.
   * In these ktrs query paths had to be specified a second time in the transform in order to get
   * them into the update/upsert object. Now we assume that there will never be a situation where
   * the user might not want the match fields present in the update object
   *
   * @throws HopException
   */
  @Test
  public void testUpdateObjectBackwardsCompatibility() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    mf.m_updateMatchField = true;
    paths.add(mf);

    // same as previous field (but not a match condition). Prior to PDI-11045 the
    // user had to specify the match conditions a second time (but not marked in the
    // transform as a match condition) in order to get them into the update/upsert object
    mf = mf("field1", true, "");
    mf.m_updateMatchField = false;
    paths.add(mf);

    mf = mf("field2", true, "");
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    // here we expect that field1 does *not* occur twice in the update object
    assertEquals(JSON.serialize(result), "{ \"field1\" : \"value1\" , \"field2\" : 12}");
  }

  @Test
  public void testTopLevelObjectStructureNoNestedDocs() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, ""), mf("field2", true, ""));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    assertEquals(JSON.serialize(result), "{ \"field1\" : \"value1\" , \"field2\" : 12}");
  }

  @Test
  public void testTopLevelArrayStructureWithPrimitives() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", false, "[0]"), mf("field2", false, "[1]"));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);

    assertEquals(JSON.serialize(result), "[ \"value1\" , 12]");
  }

  @Test
  public void testTopLevelArrayStructureWithObjects() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, "[0]"), mf("field2", true, "[1]"));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);

    assertEquals(JSON.serialize(result), "[ { \"field1\" : \"value1\"} , { \"field2\" : 12}]");
  }

  @Test
  public void testTopLevelArrayStructureContainingOneObjectMutipleFields() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, "[0]"), mf("field2", true, "[0]"));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);

    assertEquals(JSON.serialize(result), "[ { \"field1\" : \"value1\" , \"field2\" : 12}]");
  }

  @Test
  public void testTopLevelArrayStructureContainingObjectWithArray() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0].inner[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[0].inner[1]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);

    assertEquals(
        JSON.serialize(result),
        "[ { \"inner\" : [ { \"field1\" : \"value1\"} , { \"field2\" : 12}]}]");
  }

  @Test
  public void testTopLevelObjectStructureOneLevelNestedDoc() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("field1", true, ""), mf("field2", true, "nestedDoc"));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    assertEquals(
        JSON.serialize(result), "{ \"field1\" : \"value1\" , \"nestedDoc\" : { \"field2\" : 12}}");
  }

  @Test
  public void testTopLevelObjectStructureTwoLevelNested() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "nestedDoc.secondNested";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    assertEquals(
        JSON.serialize(result),
        "{ \"nestedDoc\" : { \"secondNested\" : { \"field1\" : \"value1\"} , \"field2\" : 12}}");
  }

  @Test
  public void testModifierUpdateWithMultipleModifiersOfSameType() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );
    MongoDbOutputData data = new MongoDbOutputData();

    IVariables vars = new Variables();
    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("field2", true, "nestedDoc");
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaString("field2"));

    Object[] dummyRow = new Object[] {"value1", "value2"};

    // test to see that having more than one path specify the $set operation
    // results
    // in an update object with two entries
    DBObject modifierUpdate =
        data.getModifierUpdateObject(
            paths, rm, dummyRow, vars, MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$set") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$set");
    assertEquals(setOpp.keySet().size(), 2);
  }

  @Test
  public void testModifierSetComplexArrayGrouping() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );
    MongoDbOutputData data = new MongoDbOutputData();

    IVariables vars = new Variables();
    MongoDbOutputMeta.MongoField mf = mf("field1", true, "bob.fred[0].george");
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("field2", true, "bob.fred[0].george");
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaString("field2"));

    Object[] dummyRow = new Object[] {"value1", "value2"};

    DBObject modifierUpdate =
        data.getModifierUpdateObject(
            paths, rm, dummyRow, vars, MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$set") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$set");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" as the first
    // entry in
    // the array - so there should be one entry for $set
    assertEquals(setOpp.keySet().size(), 1);

    // check the resulting update structure
    assertEquals(
        JSON.serialize(modifierUpdate),
        "{ \"$set\" : { \"bob.fred\" : [ { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\"}}]}}");
  }

  @Test
  public void testModifierPushComplexObject() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );
    MongoDbOutputData data = new MongoDbOutputData();

    IVariables vars = new Variables();

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("field2", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaString("field2"));

    Object[] dummyRow = new Object[] {"value1", "value2"};

    DBObject modifierUpdate =
        data.getModifierUpdateObject(
            paths, rm, dummyRow, vars, MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$push") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$push");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" for $push to
    // append
    // to the end of the array
    assertEquals(setOpp.keySet().size(), 1);

    assertEquals(
        JSON.serialize(modifierUpdate),
        "{ \"$push\" : { \"bob.fred\" : { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\"}}}}");
  }

  @Test
  public void testModifierPushComplexObjectWithJsonNestedDoc() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 3 );
    MongoDbOutputData data = new MongoDbOutputData();

    IVariables vars = new Variables();

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("field2", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("jsonField", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.m_JSON = true;
    mf.init(vars);
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaString("field2"));
    rm.addValueMeta(new ValueMetaString("jsonField"));

    Object[] dummyRow =
        new Object[] {"value1", "value2", "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}"};

    DBObject modifierUpdate =
        data.getModifierUpdateObject(
            paths, rm, dummyRow, vars, MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$push") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$push");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" for $push to
    // append
    // to the end of the array
    assertEquals(setOpp.keySet().size(), 1);

    assertEquals(
        JSON.serialize(modifierUpdate),
        "{ \"$push\" : { \"bob.fred\" : { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\" , "
            + "\"jsonField\" : "
            + "{ \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}}}}}");
  }

  @Test
  public void testInsertHopFieldThatContainsJsonIntoTopLevelRecord() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 3 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    paths.add(mf);

    mf = mf("field2", true, "");
    paths.add(mf);

    mf = mf("jsonField", true, "");
    mf.m_JSON = true;
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaInteger("field2"));
    rm.addValueMeta(new ValueMetaString("jsonField"));

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = 12L;
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rm, row, MongoTopLevel.RECORD, false);

    assertEquals(
        JSON.serialize(result),
        "{ \"field1\" : \"value1\" , \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\" , "
            + "\"jsonDocField2\" : 42}}");
  }

  @Test
  public void testScanForInsertTopLevelJSONDocAsIs() throws Exception {
    MongoDbOutputMeta.MongoField mf = mf("", false, "");
    mf.m_JSON = true;

    assertTrue(MongoDbOutputData.scanForInsertTopLevelJSONDoc(singletonList(mf)));
  }

  @Test
  public void testForInsertTopLevelJSONDocAsIsWithOneJSONMatchPathAndOneJSONInsertPath()
      throws Exception {

    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = mf("", false, "");
    mf.m_JSON = true;
    paths.add(mf);

    mf = mf("", false, "");
    mf.m_updateMatchField = true;
    mf.m_JSON = true;
    paths.add(mf);

    assertTrue(MongoDbOutputData.scanForInsertTopLevelJSONDoc(paths));
  }

  @Test(expected = HopException.class)
  public void
      testScanForInsertTopLevelJSONDocAsIsWithMoreThanOnePathSpecifyingATopLevelJSONDocToInsert()
          throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );

    MongoDbOutputMeta.MongoField mf = mf("", false, "");
    mf.m_JSON = true;
    paths.add(mf);

    mf = mf("", false, "");
    mf.m_JSON = true;
    paths.add(mf);

    MongoDbOutputData.scanForInsertTopLevelJSONDoc(paths);
    fail(
        "Was expecting an exception because more than one path specifying a JSON "
            + "doc to insert as-is is not kosher");
  }

  @Test
  public void testInsertHopFieldThatContainsJsonIntoOneLevelNestedDoc() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 3 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    paths.add(mf);

    mf = mf("field2", true, "nestedDoc");
    paths.add(mf);

    mf = mf("jsonField", true, "nestedDoc");
    mf.m_JSON = true;
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));
    rmi.addValueMeta(new ValueMetaString("jsonField"));

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = 12L;
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.RECORD, false);

    assertEquals(
        JSON.serialize(result),
        "{ \"field1\" : \"value1\" , \"nestedDoc\" : { \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\""
            + " , \"jsonDocField2\" : 42}}}");
  }

  @Test
  public void testInsertHopFieldThatContainsJsonIntoTopLevelArray() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 3 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "[0]");
    paths.add(mf);

    mf = mf("field2", true, "[1]");
    paths.add(mf);

    mf = mf("jsonField", false, "[2]");
    mf.m_JSON = true;
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));
    rmi.addValueMeta(new ValueMetaString("jsonField"));

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = 12L;
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);

    assertEquals(
        JSON.serialize(result),
        "[ { \"field1\" : \"value1\"} , { \"field2\" : 12} , { \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}]");
  }

  @Test
  public void testGetQueryObjectThatContainsJsonNestedDoc() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 3 );

    MongoDbOutputMeta.MongoField mf = mf("field1", true, "");
    mf.m_updateMatchField = true;
    paths.add(mf);

    mf = mf("field2", true, "");
    mf.m_updateMatchField = true;
    paths.add(mf);

    mf = mf("jsonField", true, "");
    mf.m_updateMatchField = true;
    mf.m_JSON = true;
    paths.add(mf);

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));
    rmi.addValueMeta(new ValueMetaString("jsonField"));

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = 12L;
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    IVariables vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject query =
        MongoDbOutputData.getQueryObject(
            paths, rmi, row, vs, MongoTopLevel.RECORD);

    assertEquals(
        JSON.serialize(query),
        "{ \"field1\" : \"value1\" , \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\" , "
            + "\"jsonDocField2\" : 42}}");
  }

  /**
   * PDI-9989. If transform configuration contains mongo field names which doesn't have equivalents
   * in transform input row meta - throw exception, with detailed information.
   *
   * <p>(expected mongo field name b1 has no equivalent in input row meta)
   */
  @Test(expected = HopException.class)
  public void testTransformIsFailedIfOneOfMongoFieldsNotFound() throws Exception {
    MongoDbOutput output = prepareMongoDbOutputMock();

    final String[] metaNames = new String[] {"a1", "a2", "a3"};
    String[] mongoNames = new String[] {"b1", "a2", "a3"};
    List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields(mongoNames);
    IRowMeta rmi = getStubRowMetaInterface(metaNames);
    EasyMock.replay(output);
    try {
      output.checkInputFieldsMatch(rmi, mongoFields);
    } catch (HopException e) {
      String detailedMessage = e.getMessage();
      Assert.assertTrue(
          "Exception message mentions fields not found:", detailedMessage.contains("b1"));
      throw new HopException(e);
    }
  }

  @Test
  public void dbNameRequiredOnInit() {
    when(iTransformMeta.getDbName()).thenReturn("");
    assertFalse("init should return false on fail", dbOutput.init());
    verify(mockLog).logError(stringCaptor.capture(), throwableCaptor.capture());
    assertThat(
        stringCaptor.getValue(),
        equalTo(
            BaseMessages.getString(
                PKG, "MongoDbOutput.Messages.Error.ProblemConnecting", "host", "1010")));
  }

  @Test
  public void collectionRequiredOnInit() {
    when(iTransformMeta.getDbName()).thenReturn("dbname");
    when(iTransformMeta.getCollection()).thenReturn("");
    assertFalse("init should return false on fail", dbOutput.init());
    verify(mockLog).logError(stringCaptor.capture(), throwableCaptor.capture());
    assertThat(
        stringCaptor.getValue(),
        equalTo(
            BaseMessages.getString(
                PKG, "MongoDbOutput.Messages.Error.ProblemConnecting", "host", "1010")));
  }

  @Test
  public void testInit() {
    setupReturns();
    assertTrue(dbOutput.init());
    verify(iTransformData).setConnection(mongoClientWrapper);
    verify(iTransformData).getConnection();
  }

  @Test
  public void testSettingWriteRetriesWriteDelay() {
    setupReturns();
    when(iTransformMeta.getWriteRetries()).thenReturn("111");
    when(iTransformMeta.getWriteRetryDelay()).thenReturn("222");
    dbOutput.init();
    assertThat(dbOutput.m_writeRetries, equalTo(111));
    assertThat(dbOutput.m_writeRetryDelay, equalTo(222));

    // bad data reverts to default
    when(iTransformMeta.getWriteRetries()).thenReturn("foo");
    when(iTransformMeta.getWriteRetryDelay()).thenReturn("bar");
    dbOutput.init();
    assertThat(dbOutput.m_writeRetries, equalTo(MongoDbOutputMeta.RETRIES));
    assertThat(dbOutput.m_writeRetryDelay, equalTo(MongoDbOutputMeta.RETRY_DELAY));
  }

  private void setupReturns() {
    when(iTransformMeta.getDbName()).thenReturn("dbname");
    when(iTransformMeta.getCollection()).thenReturn("collection");
    when(iTransformMeta.getAuthenticationUser()).thenReturn("joe");
    when(iTransformData.getCollection()).thenReturn(mongoCollectionWrapper);
    when(pipeline.isRunning()).thenReturn(true);
    mongoFields = asList(mongoField("foo"), mongoField("bar"), mongoField("baz"));
    when(iTransformMeta.getMongoFields()).thenReturn(mongoFields);
    when(iTransformData.getMongoFields()).thenReturn(mongoFields);
  }

  private MongoDbOutputMeta.MongoField mongoField(String fieldName) {
    MongoDbOutputMeta.MongoField field = new MongoDbOutputMeta.MongoField();
    field.m_incomingFieldName = fieldName;
    field.m_mongoDocPath = fieldName;
    IVariables vars = mock(IVariables.class);
    when(vars.resolve(anyString())).thenReturn(fieldName);
    when(vars.resolve(anyString())).thenReturn(fieldName);
    field.init(vars);
    return field;
  }

  @Test
  public void testProcessRowNoInput() throws Exception {
    setupReturns();
    dbOutput.init();
    // no input has been defined, so should gracefully finish and clean up.
    assertFalse(dbOutput.processRow());
    verify(mongoClientWrapper).dispose();
    assertEquals(0, dbOutput.getLinesOutput());
  }

  @Test
  public void testProcessRowWithInput() throws Exception {
    setupReturns();
    setupRowMeta();
    dbOutput.init();
    assertTrue(dbOutput.processRow());
  }

  @Test
  public void testProcessRowPassRowsToNextTransform() throws Exception {
    setupReturns();
    setupRowMeta();
    dbOutput.init();
    assertTrue(dbOutput.processRow());
    assertTrue(dbOutput.processRow());
    assertTrue(dbOutput.processRow());
    assertEquals(3, outputRowData.size());
    assertEquals(3, dbOutput.getLinesOutput());
  }

  private void setupRowMeta() {
    rowData = new Object[] {"foo", "bar", "baz"};
    outputRowData = new ArrayList<>();
    rowMeta.addValueMeta(new ValueMetaString("foo"));
    rowMeta.addValueMeta(new ValueMetaString("bar"));
    rowMeta.addValueMeta(new ValueMetaString("baz"));
  }

  @Test
  public void testUpdate() throws Exception {
    setupReturns();
    WriteResult result = mock(WriteResult.class);
    CommandResult commandResult = mock(CommandResult.class);
    when(commandResult.ok()).thenReturn(true);
    when(mongoCollectionWrapper.update(
            any(DBObject.class), any(DBObject.class), anyBoolean(), anyBoolean()))
        .thenReturn(result);
    when(iTransformMeta.getUpdate()).thenReturn(true);

    // flag a field for update = "foo"
    MongoDbOutputMeta.MongoField mongoField = mongoFields.get(0);
    mongoField.m_updateMatchField = true;

    setupRowMeta();
    dbOutput.init();
    assertTrue(dbOutput.processRow());
    ArgumentCaptor<BasicDBObject> updateQueryCaptor = ArgumentCaptor.forClass(BasicDBObject.class);
    ArgumentCaptor<BasicDBObject> insertCaptor = ArgumentCaptor.forClass(BasicDBObject.class);

    // update is executed
    verify(mongoCollectionWrapper)
        .update(updateQueryCaptor.capture(), insertCaptor.capture(), anyBoolean(), anyBoolean());
    // updated field is expected
    assertThat(updateQueryCaptor.getValue(), equalTo(new BasicDBObject("foo", "foo")));
    // insert document is expected
    assertThat(
        insertCaptor.getValue(),
        equalTo(new BasicDBObject((ImmutableMap.of("foo", "foo", "bar", "bar", "baz", "baz")))));
  }

  @Test
  public void updateFailureRetries() throws Exception {
    setupReturns();
    setupRowMeta();
    // retry twice with no delay
    when(iTransformMeta.getWriteRetries()).thenReturn("2");
    when(iTransformMeta.getWriteRetryDelay()).thenReturn("0");
    when(iTransformMeta.getUpdate()).thenReturn(true);
    MongoDbOutputMeta.MongoField mongoField = mongoFields.get(0);
    mongoField.m_updateMatchField = true;

    when(mongoCollectionWrapper.update(
            any(DBObject.class), any(DBObject.class), anyBoolean(), anyBoolean()))
        .thenThrow(mock(MongoDbException.class));

    dbOutput.init();
    try {
      dbOutput.processRow();
      fail("expected exception");
    } catch (HopException ke) {
      // update should be called 3 times (first time plus 2 retries)
      verify(mongoCollectionWrapper, times(3))
          .update(any(DBObject.class), any(DBObject.class), anyBoolean(), anyBoolean());
    }
  }

  @Test
  public void doBatchWithRetry() throws Exception {
    setupReturns();
    setupRowMeta();
    dbOutput.m_batch = new ArrayList<>();
    dbOutput.m_batch.add(
        new BasicDBObject(ImmutableMap.of("foo", "fooval", "bar", "barval", "baz", "bazval")));
    List<Object[]> batchRows = new ArrayList<>();
    batchRows.add(rowData);

    List<DBObject> batchCopy = new ArrayList(dbOutput.m_batch);

    dbOutput.m_batchRows = batchRows;
    when(iTransformMeta.getWriteRetries()).thenReturn("1");
    when(iTransformMeta.getWriteRetryDelay()).thenReturn("0");
    WriteResult result = mock(WriteResult.class);
    CommandResult commandResult = mock(CommandResult.class);
    when(commandResult.ok()).thenReturn(true);
    when(mongoCollectionWrapper.save(dbOutput.m_batch.get(0))).thenReturn(result);

    doThrow(mock(MongoException.class)).when(mongoCollectionWrapper).insert(anyList());
    dbOutput.init();
    dbOutput.doBatch();

    // should attempt insert once, falling back to save on retry
    verify(mongoCollectionWrapper, times(1)).insert(anyList());
    verify(mongoCollectionWrapper, times(1)).save(batchCopy.get(0));

    // batch should be cleared.
    assertThat(dbOutput.m_batch.size(), equalTo(0));
    assertThat(dbOutput.m_batchRows.size(), equalTo(0));
  }

  @Test
  public void testDisposeFailureLogged() throws MongoDbException {
    doThrow(new MongoDbException()).when(mongoClientWrapper).dispose();
    dbOutput.init();
    dbOutput.dispose();
    verify(mockLog).logError(anyString());
  }

  /**
   * Tests if mongo output configuration contains excessive fields in transform input against mongo
   * output fields, we generate a mockLog record about the fields will not be used in mongo output.
   *
   * @throws Exception
   */
  @Test
  public void testTransformLogSkippedFields() throws Exception {
    MongoDbOutput output = prepareMongoDbOutputMock();

    final String[] metaNames = new String[] {"a1", "a2", "a3"};
    String[] mongoNames = new String[] {"a1", "a2"};
    Capture<String> loggerCapture = new Capture<>( CaptureType.ALL );
    output.logBasic(EasyMock.capture(loggerCapture));
    EasyMock.replay(output);
    IRowMeta rmi = getStubRowMetaInterface(metaNames);
    List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields(mongoNames);

    output.checkInputFieldsMatch(rmi, mongoFields);
    List<String> logRecords = loggerCapture.getValues();

    Assert.assertEquals("We have one mockLog record generated", 1, logRecords.size());
    Assert.assertTrue(
        "We have a mockLog record mentions that 'a3' field will not be used.",
        logRecords.get(0).contains("a3"));
  }

  /**
   * Tests if none mongo output fields exists - transform is failed.
   *
   * @throws Exception
   */
  @Test(expected = HopException.class)
  public void testTransformFailsIfNoMongoFieldsFound() throws Exception {
    MongoDbOutput output = prepareMongoDbOutputMock();

    final String[] metaNames = new String[] {"a1", "a2"};
    String[] mongoNames = new String[0];
    EasyMock.replay(output);
    IRowMeta rmi = getStubRowMetaInterface(metaNames);
    List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields(mongoNames);
    output.checkInputFieldsMatch(rmi, mongoFields);
  }

  @Test
  public void testTransformMetaSetsUpdateInXML() throws Exception {
    MongoDbOutputMeta mongoMeta = new MongoDbOutputMeta();
    mongoMeta.setUpdate(true);

    String XML = mongoMeta.getXml();
    XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<transform>\n" + XML + "\n</transform>\n";

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new ByteArrayInputStream(XML.getBytes("UTF-8")));

    NodeList transform = doc.getElementsByTagName("transform");
    Node transformNode = transform.item(0);

    MongoDbOutputMeta newMeta = new MongoDbOutputMeta();
    newMeta.loadXml(transformNode, null);

    assertTrue(newMeta.getUpdate());
    assertFalse(newMeta.getUpsert());
  }

  @Test
  public void testTransformMetaSettingUpsertAlsoSetsUpdateInXML() throws Exception {
    MongoDbOutputMeta mongoMeta = new MongoDbOutputMeta();
    mongoMeta.setUpsert(true);

    String XML = mongoMeta.getXml();
    XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<transform>\n" + XML + "\n</transform>\n";

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new ByteArrayInputStream(XML.getBytes("UTF-8")));

    NodeList transform = doc.getElementsByTagName("transform");
    Node transformNode = transform.item(0);

    MongoDbOutputMeta newMeta = new MongoDbOutputMeta();
    newMeta.loadXml(transformNode, null);

    assertTrue(newMeta.getUpsert());
    assertTrue(newMeta.getUpdate());
  }

  private MongoDbOutput prepareMongoDbOutputMock() {
    MongoDbOutput output = EasyMock.createNiceMock(MongoDbOutput.class);
    EasyMock.expect(output.resolve(EasyMock.anyObject(String.class)))
        .andAnswer(
            () -> {
              Object[] args = EasyMock.getCurrentArguments();
              String ret = String.valueOf(args[0]);
              return ret;
            })
        .anyTimes();
    return output;
  }

  private IRowMeta getStubRowMetaInterface(final String[] metaNames) {
    IRowMeta rmi = EasyMock.createNiceMock(IRowMeta.class);
    EasyMock.expect(rmi.getFieldNames()).andReturn(metaNames).anyTimes();
    EasyMock.expect(rmi.size()).andReturn(metaNames.length).anyTimes();
    EasyMock.expect(rmi.getValueMeta(EasyMock.anyInt()))
        .andAnswer(
            () -> {
              Object[] args = EasyMock.getCurrentArguments();
              int i = Integer.class.cast(args[0]);
              IValueMeta ret = new ValueMetaString(metaNames[i]);
              return ret;
            })
        .anyTimes();
    EasyMock.replay(rmi);
    return rmi;
  }

  private static MongoDbOutputMeta.MongoField mf(
      String incomingFieldName, boolean useIncomingName, String docPath) {
    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = incomingFieldName;
    mf.m_useIncomingFieldNameAsMongoFieldName = useIncomingName;
    mf.m_mongoDocPath = docPath;
    return mf;
  }

  private List<MongoDbOutputMeta.MongoField> getMongoFields(String[] names) {
    List<MongoDbOutputMeta.MongoField> ret =
      new ArrayList<>( names.length );
    for (String name : names) {
      MongoDbOutputMeta.MongoField field = new MongoDbOutputMeta.MongoField();
      field.m_incomingFieldName = name;
      field.environUpdatedFieldName = name;
      ret.add(field);
    }
    return ret;
  }

  @Test
  public void getModifierUpdateObject_PicksUpNull_WhenPermitted() throws Exception {

    IVariables vs = new Variables();

    MongoDbOutputMeta.MongoField permittedNull = mf("permittedNull", true, "");
    permittedNull.m_modifierUpdateOperation = "$set";
    permittedNull.m_modifierOperationApplyPolicy = "Insert&Update";
    permittedNull.insertNull = true;
    permittedNull.init(vs);

    MongoDbOutputMeta.MongoField prohibitedNull = mf("prohibitedNull", true, "");
    prohibitedNull.m_modifierUpdateOperation = "$set";
    prohibitedNull.m_modifierOperationApplyPolicy = "Insert&Update";
    prohibitedNull.insertNull = false;
    prohibitedNull.init(vs);

    MongoDbOutputMeta.MongoField anotherField = mf("anotherField", true, "");
    anotherField.m_modifierUpdateOperation = "$set";
    anotherField.m_modifierOperationApplyPolicy = "Insert&Update";
    anotherField.init(vs);

    List<MongoDbOutputMeta.MongoField> paths = asList(permittedNull, prohibitedNull, anotherField);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("permittedNull"));
    rm.addValueMeta(new ValueMetaString("prohibitedNull"));
    rm.addValueMeta(new ValueMetaString("anotherField"));

    Object[] row = new Object[] {null, null, "qwerty"};

    DBObject updateObject =
        new MongoDbOutputData()
            .getModifierUpdateObject(paths, rm, row, vs, MongoTopLevel.RECORD);

    assertNotNull(updateObject);

    DBObject setOpp = (DBObject) updateObject.get("$set");
    assertNotNull(setOpp);

    assertEquals("'permittedNull' and 'anotherField' are expected", setOpp.keySet().size(), 2);
    assertNull(setOpp.get("permittedNull"));
    assertEquals("qwerty", setOpp.get("anotherField"));
  }

  /**
   * Testing the use of Environment Substitution during initialization of fields.
   *
   * @throws Exception
   */
  @Test
  public void testTopLevelArrayWithEnvironmentSubstitution() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths =
        asList(mf("${ENV_FIELD}", true, "[0]"), mf("field2", true, "${ENV_DOC_PATH}"));

    IRowMeta rmi = new RowMeta();
    rmi.addValueMeta(new ValueMetaString("field1"));
    rmi.addValueMeta(new ValueMetaInteger("field2"));

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = 12L;
    IVariables vs = new Variables();
    vs.setVariable("ENV_FIELD", "field1");
    vs.setVariable("ENV_DOC_PATH", "[1]");

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = hopRowToMongo(paths, rmi, row, MongoTopLevel.ARRAY, false);
    assertEquals(JSON.serialize(result), "[ { \"field1\" : \"value1\"} , { \"field2\" : 12}]");
  }

  /**
   * Testing the use of Environment Substitution during initialization of fields.
   *
   * @throws Exception
   */
  @Test
  public void testModifierPushObjectWithEnvironmentSubstitution() throws Exception {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<>( 2 );
    MongoDbOutputData data = new MongoDbOutputData();

    IVariables vars = new Variables();
    vars.setVariable("ENV_FIELD", "field1");
    vars.setVariable("ENV_DOC_PATH", "bob.fred[].george");
    vars.setVariable("ENV_UPDATE_OP", "$push");

    MongoDbOutputMeta.MongoField mf = mf("${ENV_FIELD}", true, "${ENV_DOC_PATH}");
    mf.m_modifierUpdateOperation = "${ENV_UPDATE_OP}";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    mf = mf("field2", true, "bob.fred[].george");
    mf.m_modifierUpdateOperation = "${ENV_UPDATE_OP}";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.init(vars);
    paths.add(mf);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("field1"));
    rm.addValueMeta(new ValueMetaString("field2"));

    Object[] dummyRow = new Object[] {"value1", "value2"};

    DBObject modifierUpdate =
        data.getModifierUpdateObject(
            paths, rm, dummyRow, vars, MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$push") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$push");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" for $push to
    // append
    // to the end of the array
    assertEquals(setOpp.keySet().size(), 1);

    assertEquals(
        JSON.serialize(modifierUpdate),
        "{ \"$push\" : { \"bob.fred\" : { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\"}}}}");
  }
}
