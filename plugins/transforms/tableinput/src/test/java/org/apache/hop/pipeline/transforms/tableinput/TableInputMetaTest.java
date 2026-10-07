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
 *
 */

package org.apache.hop.pipeline.transforms.tableinput;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TableInputMetaTest {
  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    TableInputMeta meta = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        TableInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TableInputMeta metaCopy = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        TableInputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(TableInputMeta meta) {
    Assertions.assertEquals("h2", meta.getConnection());
    Assertions.assertEquals("100", meta.getRowLimit());
    Assertions.assertEquals("parameters", meta.getLookup());
    Assertions.assertTrue(meta.isExecuteEachInputRow());
    Assertions.assertTrue(meta.isVariableReplacementActive());
    Assertions.assertEquals("SELECT ID, NAME FROM PUBLIC.DDLTEST WHERE NAME = ?", meta.getSql());
    Assertions.assertFalse(meta.isUseNamedParameters());
    Assertions.assertTrue(meta.isSpecifyFields());
    Assertions.assertTrue(meta.isValidateSpecifiedFields());
    Assertions.assertEquals(2, meta.getFields().size());
    Assertions.assertEquals("ID", meta.getFields().get(0).getName());
    Assertions.assertEquals(IValueMeta.TYPE_INTEGER, meta.getFields().get(0).getType());
    Assertions.assertEquals("NAME", meta.getFields().get(1).getName());
    Assertions.assertEquals(IValueMeta.TYPE_STRING, meta.getFields().get(1).getType());

    // Do we have an IO stream?
    Assertions.assertFalse(meta.getTransformIOMeta().getInfoStreams().isEmpty());
    IStream stream = meta.getTransformIOMeta().getInfoStreams().get(0);
    Assertions.assertNotNull(stream);
    Assertions.assertEquals("parameters", stream.getSubject());
  }

  @Test
  void newTransformEnablesNamedParameters() {
    Assertions.assertTrue(new TableInputMeta().isUseNamedParameters());
  }

  @Test
  void getFieldsUsesSpecifiedFieldsWithoutConnection() throws Exception {
    TableInputMeta meta = new TableInputMeta();
    meta.setSpecifyFields(true);
    TableInputField id = new TableInputField();
    id.setName("id");
    id.setType(IValueMeta.TYPE_INTEGER);
    TableInputField name = new TableInputField();
    name.setName("name");
    name.setType(IValueMeta.TYPE_STRING);
    meta.getFields().add(id);
    meta.getFields().add(name);

    IRowMeta row = new RowMeta();
    meta.getFields(row, "Table input", null, null, new Variables(), null);

    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals("id", row.getValueMeta(0).getName());
    Assertions.assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(0).getType());
    Assertions.assertEquals("Table input", row.getValueMeta(0).getOrigin());
    Assertions.assertEquals("name", row.getValueMeta(1).getName());
  }

  @Test
  void cloneCopiesSpecifiedFields() {
    TableInputMeta meta = new TableInputMeta();
    meta.setUseNamedParameters(false);
    meta.setSpecifyFields(true);
    meta.setValidateSpecifiedFields(true);
    TableInputField field = new TableInputField();
    field.setName("id");
    field.setType(IValueMeta.TYPE_INTEGER);
    meta.getFields().add(field);

    TableInputMeta copy = (TableInputMeta) meta.clone();
    Assertions.assertFalse(copy.isUseNamedParameters());
    Assertions.assertTrue(copy.isSpecifyFields());
    Assertions.assertTrue(copy.isValidateSpecifiedFields());
    Assertions.assertEquals(1, copy.getFields().size());
    Assertions.assertEquals("id", copy.getFields().get(0).getName());
    copy.getFields().get(0).setName("other");
    Assertions.assertEquals("id", meta.getFields().get(0).getName());
  }

  @Test
  void specifiedMappingMatchesByName() throws Exception {
    TableInputMeta meta = new TableInputMeta();
    IRowMeta jdbc = new RowMeta();
    jdbc.addValueMeta(new ValueMetaString("extra"));
    jdbc.addValueMeta(new ValueMetaString("key"));
    jdbc.addValueMeta(new ValueMetaString("value"));

    IRowMeta specified = new RowMeta();
    specified.addValueMeta(new ValueMetaString("key"));
    specified.addValueMeta(new ValueMetaString("value"));

    int[] mapping = meta.createSpecifiedMapping(jdbc, specified, true);
    Assertions.assertArrayEquals(new int[] {1, 2}, mapping);
  }

  @Test
  void specifiedMappingValidateTypeMismatch() {
    TableInputMeta meta = new TableInputMeta();
    IRowMeta jdbc = new RowMeta();
    jdbc.addValueMeta(new ValueMetaString("key"));
    IRowMeta specified = new RowMeta();
    specified.addValueMeta(new ValueMetaInteger("key"));
    Assertions.assertThrows(
        Exception.class, () -> meta.createSpecifiedMapping(jdbc, specified, true));
  }

  @Test
  void specifiedMappingAllowsTypeConversionWhenNotValidating() throws Exception {
    TableInputMeta meta = new TableInputMeta();
    IRowMeta jdbc = new RowMeta();
    jdbc.addValueMeta(new ValueMetaString("key"));
    IRowMeta specified = new RowMeta();
    specified.addValueMeta(new ValueMetaInteger("key"));
    int[] mapping = meta.createSpecifiedMapping(jdbc, specified, false);
    Assertions.assertArrayEquals(new int[] {0}, mapping);
  }

  @Test
  void newTransformDoesNotExecuteEachRowByDefault() {
    Assertions.assertFalse(new TableInputMeta().isExecuteEachInputRow());
  }

  @Test
  void loadSaveKeepsEmptyLookupAndNamedParameters() throws Exception {
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getResource("/transform-named-no-lookup.xml"))
                .toURI());
    String xml = Files.readString(path);
    TableInputMeta meta = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        TableInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    Assertions.assertTrue(Utils.isEmpty(meta.getLookup()));
    Assertions.assertFalse(meta.isExecuteEachInputRow());
    Assertions.assertTrue(meta.isUseNamedParameters());
    Assertions.assertEquals(
        "SELECT ID, NAME FROM PUBLIC.DDLTEST WHERE NAME = {name}", meta.getSql());

    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TableInputMeta metaCopy = new TableInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        TableInputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    Assertions.assertTrue(Utils.isEmpty(metaCopy.getLookup()));
    Assertions.assertFalse(metaCopy.isExecuteEachInputRow());
    Assertions.assertTrue(metaCopy.isUseNamedParameters());
  }

  @Test
  void getFieldsDoesNotKeepIncomingParameterFields() throws Exception {
    TableInputMeta meta = new TableInputMeta();
    meta.setSpecifyFields(true);
    TableInputField id = new TableInputField();
    id.setName("id");
    id.setType(IValueMeta.TYPE_INTEGER);
    meta.getFields().add(id);

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("incoming"));
    meta.getFields(row, "Table input", null, null, new Variables(), null);

    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals("id", row.getValueMeta(0).getName());
    Assertions.assertEquals(-1, row.indexOfValue("incoming"));
  }

  @Test
  void parameterRowMetaUsesPrevWhenLookupIsEmpty() {
    TableInputMeta meta = new TableInputMeta();
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("name"));

    IRowMeta result = meta.parameterRowMeta(null, prev);

    Assertions.assertSame(prev, result);
  }

  @Test
  void parameterRowMetaPrefersInfoStreamWhenPresent() {
    TableInputMeta meta = new TableInputMeta();
    meta.setLookup("parameters");
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("other"));
    IRowMeta info = new RowMeta();
    info.addValueMeta(new ValueMetaInteger("id"));

    IRowMeta result = meta.parameterRowMeta(new IRowMeta[] {info}, prev);

    Assertions.assertSame(info, result);
  }

  @Test
  void checkNamedParametersWithIncomingHopsAndEmptyLookup() {
    TableInputMeta meta = namedParameterMeta();
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("key"));

    List<ICheckResult> remarks = runCheck(meta, prev, new String[] {"source"}, new RowMeta());

    Assertions.assertTrue(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_OK,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.NamedParametersOk", "1")));
    Assertions.assertTrue(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_OK,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.ReadsIncomingHops")));
    Assertions.assertFalse(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.NamedParametersNeedIncoming")));
  }

  @Test
  void checkNamedParametersWithoutIncomingHops() {
    TableInputMeta meta = namedParameterMeta();

    List<ICheckResult> remarks = runCheck(meta, new RowMeta(), new String[0], new RowMeta());

    Assertions.assertTrue(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.NamedParametersNeedIncoming")));
  }

  @Test
  void checkMissingNamedParameterField() {
    TableInputMeta meta = namedParameterMeta();
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("other"));

    List<ICheckResult> remarks = runCheck(meta, prev, new String[] {"source"}, new RowMeta());

    Assertions.assertTrue(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.NamedParametersMissing", "key")));
  }

  @Test
  void checkLookupNameIsOptionalWhenOtherHopsExist() {
    TableInputMeta meta = namedParameterMeta();
    meta.setLookup("missing-lookup");
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("key"));

    List<ICheckResult> remarks = runCheck(meta, prev, new String[] {"source"}, new RowMeta());

    Assertions.assertFalse(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("missing-lookup")));
    Assertions.assertTrue(
        hasRemark(
            remarks,
            ICheckResult.TYPE_RESULT_OK,
            BaseMessages.getString(
                TableInputMeta.class, "TableInputMeta.CheckResult.ReadsIncomingHops")));
  }

  @Test
  void checkPositionalParametersFromIncomingHops() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("h2");
    meta.setUseNamedParameters(false);
    meta.setSql("SELECT * FROM t WHERE bar IN (?,?,?)");
    IRowMeta assembled = new RowMeta();
    assembled.addValueMeta(new ValueMetaInteger("bar"));
    assembled.addValueMeta(new ValueMetaInteger("bar"));
    assembled.addValueMeta(new ValueMetaInteger("bar"));

    List<ICheckResult> remarks = runCheck(meta, assembled, new String[] {"source"}, new RowMeta());

    Assertions.assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_OK
                        && r.getText().contains("receiving 3")
                        && r.getText().contains("3 fields")));
  }

  @Test
  void getFieldsReportsUnresolvedConnectionVariable() {
    // A connection name which is a variable that isn't set at design time used to fail with a
    // NullPointerException deep inside the Database object.  See issue #8203.
    //
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("${connection_name}");
    meta.setSql("SELECT * FROM t");

    HopTransformException e =
        Assertions.assertThrows(
            HopTransformException.class,
            () ->
                meta.getFields(
                    new RowMeta(),
                    "Table input",
                    null,
                    null,
                    new Variables(),
                    new MemoryMetadataProvider()));
    Assertions.assertTrue(e.getMessage().contains("${connection_name}"), e.getMessage());
  }

  /**
   * A connection that is not in the project is reported once, by the pipeline check, under its
   * error code. Table input used to add a remark of its own, without a code, saying the same thing.
   */
  @Test
  void aMissingConnectionIsReportedOnceByPipelineVerify() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("doesnotexist");
    meta.setSql("SELECT 1");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Table input", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());

    List<ICheckResult> aboutTheConnection =
        remarks.stream()
            .filter(r -> r.getType() != ICheckResult.TYPE_RESULT_OK)
            .filter(r -> r.getText().toLowerCase().contains("connection"))
            .toList();
    Assertions.assertEquals(1, aboutTheConnection.size(), aboutTheConnection.toString());
    Assertions.assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST,
        aboutTheConnection.get(0).getErrorCode());
  }

  /**
   * The pipeline check stays silent when a connection name still holds a variable after resolving:
   * at design time that name cannot be decided. Table input can decide it - it tried to load the
   * connection with these variables and got nothing - so it reports, and the file is not left with
   * no remark at all.
   */
  @Test
  void anUnresolvedConnectionVariableIsStillReported() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("${DB_CONN}");
    meta.setSql("SELECT 1");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Table input", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());

    List<ICheckResult> aboutTheConnection =
        remarks.stream()
            .filter(r -> r.getType() != ICheckResult.TYPE_RESULT_OK)
            .filter(r -> r.getText().toLowerCase().contains("connection"))
            .toList();
    Assertions.assertEquals(1, aboutTheConnection.size(), aboutTheConnection.toString());
    Assertions.assertEquals(
        ICheckResult.TYPE_RESULT_ERROR, aboutTheConnection.get(0).getType(), "must stay an error");
    Assertions.assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_NOT_RESOLVED,
        aboutTheConnection.get(0).getErrorCode());
    Assertions.assertTrue(
        aboutTheConnection.get(0).getText().contains("${DB_CONN}"),
        aboutTheConnection.get(0).getText());
  }

  /** A connection that resolves and exists is not reported by either check. */
  @Test
  void aResolvedConnectionVariableIsNotReported() {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    Variables variables = new Variables();
    variables.setVariable("DB_CONN", "doesnotexist");

    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("${DB_CONN}");
    meta.setSql("SELECT 1");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Table input", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(remarks, false, null, variables, metadataProvider);

    // The variable resolves, so the name can be decided: the pipeline check owns it again, under
    // its own code, and table input adds nothing.
    List<ICheckResult> aboutTheConnection =
        remarks.stream()
            .filter(r -> r.getType() != ICheckResult.TYPE_RESULT_OK)
            .filter(r -> r.getText().toLowerCase().contains("connection"))
            .toList();
    Assertions.assertEquals(1, aboutTheConnection.size(), aboutTheConnection.toString());
    Assertions.assertEquals(
        ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST,
        aboutTheConnection.get(0).getErrorCode());
  }

  /**
   * A connection that was never assigned is null, not empty: the field default on a new transform.
   * It is reported once, by the pipeline check, under its own code. Table input used to add an
   * uncoded error of its own - the load of a null name raised "you need to specify the name..." -
   * which no suppression of CONNECTION_NOT_ASSIGNED could clear.
   */
  @Test
  void anUnassignedConnectionIsReportedOnceByPipelineVerify() {
    assertConnectionRemark(null, ReferencedDatabaseConnectionChecker.ERROR_NOT_ASSIGNED);
  }

  /** An empty connection name is the same thing said differently, and reported the same way. */
  @Test
  void anEmptyConnectionIsReportedOnceByPipelineVerify() {
    assertConnectionRemark("", ReferencedDatabaseConnectionChecker.ERROR_NOT_ASSIGNED);
  }

  private static void assertConnectionRemark(String connection, String expectedErrorCode) {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection(connection);
    meta.setSql("SELECT 1");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta("Table input", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    pipelineMeta.checkTransforms(
        remarks, false, null, new Variables(), new MemoryMetadataProvider());

    List<ICheckResult> aboutTheConnection =
        remarks.stream()
            .filter(r -> r.getType() != ICheckResult.TYPE_RESULT_OK)
            .filter(r -> r.getText().toLowerCase().contains("connection"))
            .toList();
    Assertions.assertEquals(1, aboutTheConnection.size(), aboutTheConnection.toString());
    Assertions.assertEquals(expectedErrorCode, aboutTheConnection.get(0).getErrorCode());
  }

  private static TableInputMeta namedParameterMeta() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("h2");
    meta.setUseNamedParameters(true);
    meta.setSql("SELECT * FROM t WHERE id = {key}");
    return meta;
  }

  private static List<ICheckResult> runCheck(
      TableInputMeta meta, IRowMeta prev, String[] input, IRowMeta info) {
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("Table input", meta);
    meta.check(
        remarks,
        new PipelineMeta(),
        transformMeta,
        prev,
        input,
        new String[0],
        info,
        new Variables(),
        new MemoryMetadataProvider());
    return remarks;
  }

  private static boolean hasRemark(List<ICheckResult> remarks, int type, String text) {
    return remarks.stream().anyMatch(r -> r.getType() == type && text.equals(r.getText()));
  }
}
