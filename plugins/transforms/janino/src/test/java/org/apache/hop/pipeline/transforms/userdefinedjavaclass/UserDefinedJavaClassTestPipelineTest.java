/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.GeneratorField;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassMeta.FieldInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The "Test class" button runs the class in a throwaway pipeline. A class reading from an info
 * transform or writing to a target transform must find those there too (#4584).
 */
class UserDefinedJavaClassTestPipelineTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String UDJC_NAME = "udjc";
  private static final String INFO_NAME = "lookup";
  private static final String TARGET_NAME = "out";

  private static final String CLASS_SOURCE =
      """
      private long infoRows;

      public boolean processRow() throws HopException {
        if (first) {
          first = false;
          IRowSet info = findInfoRowSet("info");
          while (getRowFrom(info) != null) {
            infoRows++;
          }
          // Throws when the target transform can't be found
          findTargetRowSet("target");
        }
        Object[] r = getRow();
        if (r == null) {
          setOutputDone();
          return false;
        }
        r = createOutputRow(r, data.outputRowMeta.size());
        get(Fields.Out, "info_rows").setValue(r, Long.valueOf(infoRows));
        putRow(data.outputRowMeta, r);
        return true;
      }
      """;

  private final IVariables variables = new Variables();
  private PipelineMeta sourcePipelineMeta;
  private UserDefinedJavaClassMeta udjcMeta;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    udjcMeta = new UserDefinedJavaClassMeta();
    udjcMeta.replaceDefinitions(
        List.of(new UserDefinedJavaClassDef(ClassType.TRANSFORM_CLASS, "Processor", CLASS_SOURCE)));
    udjcMeta.setFieldInfo(
        new ArrayList<>(List.of(new FieldInfo("info_rows", IValueMeta.TYPE_INTEGER, -1, -1))));

    InfoTransformDefinition infoDefinition = new InfoTransformDefinition();
    infoDefinition.setTag("info");
    infoDefinition.setTransformName(INFO_NAME);
    udjcMeta.getInfoTransformDefinitions().add(infoDefinition);

    TargetTransformDefinition targetDefinition = new TargetTransformDefinition();
    targetDefinition.tag = "target";
    targetDefinition.transformName = TARGET_NAME;
    udjcMeta.getTargetTransformDefinitions().add(targetDefinition);

    // The pipeline the transform lives in: main input, info input and target output.
    //
    sourcePipelineMeta = new PipelineMeta();
    TransformMeta main =
        addTransform(generator(field("id", "Integer", "1"), field("name", "String", "x")), "main");
    TransformMeta lookup =
        addTransform(
            generator(field("key", "String", "k"), field("value", "Integer", "2")), INFO_NAME);
    TransformMeta udjc = addTransform(udjcMeta, UDJC_NAME);
    TransformMeta out = addTransform(new DummyMeta(), TARGET_NAME);
    sourcePipelineMeta.addPipelineHop(new PipelineHopMeta(main, udjc));
    sourcePipelineMeta.addPipelineHop(new PipelineHopMeta(lookup, udjc));
    sourcePipelineMeta.addPipelineHop(new PipelineHopMeta(udjc, out));
    udjcMeta.searchInfoAndTargetTransforms(sourcePipelineMeta.getTransforms());

    udjcMeta.cookClasses();
    assertTrue(udjcMeta.getCookErrors().isEmpty(), () -> udjcMeta.getCookErrors().toString());
  }

  @Test
  void mainInputFieldsLeaveOutInfoTransforms() throws Exception {
    IRowMeta rowMeta =
        UserDefinedJavaClassTestPipeline.getMainInputFields(
            variables, sourcePipelineMeta, UDJC_NAME, udjcMeta);

    assertEquals(List.of("id", "name"), List.of(rowMeta.getFieldNames()));
  }

  @Test
  void testPipelineProvidesInfoAndTargetTransforms() throws Exception {
    IRowMeta mainRowMeta =
        UserDefinedJavaClassTestPipeline.getMainInputFields(
            variables, sourcePipelineMeta, UDJC_NAME, udjcMeta);
    RowGeneratorMeta mainGenMeta =
        UserDefinedJavaClassTestPipeline.createTestDataGenerator(mainRowMeta);

    PipelineMeta testPipelineMeta =
        UserDefinedJavaClassTestPipeline.build(
            variables, sourcePipelineMeta, UDJC_NAME, udjcMeta, mainGenMeta);

    TransformMeta infoStandIn = testPipelineMeta.findTransform(INFO_NAME);
    assertNotNull(infoStandIn);
    assertNotNull(testPipelineMeta.findTransform(TARGET_NAME));
    assertEquals(
        List.of("key", "value"),
        List.of(testPipelineMeta.getTransformFields(variables, infoStandIn).getFieldNames()));

    LocalPipelineEngine pipeline = new LocalPipelineEngine(testPipelineMeta);
    pipeline.prepareExecution();
    List<Object[]> rows = new ArrayList<>();
    List<IRowMeta> rowMetas = new ArrayList<>();
    ITransform udjc = pipeline.getTransform(UDJC_NAME, 0);
    udjc.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            rowMetas.add(rowMeta);
            rows.add(row);
          }
        });
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getErrors());
    assertEquals(10, rows.size());
    int infoRowsIndex = rowMetas.get(0).indexOfValue("info_rows");
    for (Object[] row : rows) {
      assertEquals(10L, row[infoRowsIndex]);
    }
    assertEquals(10, pipeline.getTransform(TARGET_NAME, 0).getLinesRead());
  }

  private TransformMeta addTransform(
      org.apache.hop.pipeline.transform.ITransformMeta meta, String name)
      throws HopTransformException {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    sourcePipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }

  private static RowGeneratorMeta generator(GeneratorField... fields) {
    RowGeneratorMeta meta = new RowGeneratorMeta();
    meta.setRowLimit("5");
    meta.getFields().addAll(List.of(fields));
    return meta;
  }

  private static GeneratorField field(String name, String type, String value) {
    GeneratorField field = new GeneratorField();
    field.setName(name);
    field.setType(type);
    field.setLength(-1);
    field.setPrecision(-1);
    field.setValue(value);
    return field;
  }
}
