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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PGBulkLoaderMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMeta transformMeta;
  private PGBulkLoader loader;
  private PGBulkLoaderData ld;
  private PGBulkLoaderMeta lm;

  Class<PGBulkLoaderMeta> testMetaClass = PGBulkLoaderMeta.class;

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    LoadSaveTester<PGBulkLoaderMeta> tester = new LoadSaveTester<>(PGBulkLoaderMeta.class);
    IFieldLoadSaveValidatorFactory factory = tester.getFieldLoadSaveValidatorFactory();
    factory.registerValidator(
        PGBulkLoaderMeta.class.getDeclaredField("mappings").getGenericType().toString(),
        new ListLoadSaveValidator<>(new PGBulkLoaderMappingMetaValidator()));
    tester.testSerialization();
  }

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("loader");

    lm = new PGBulkLoaderMeta();
    ld = new PGBulkLoaderData();

    PluginRegistry plugReg = PluginRegistry.getInstance();

    String loaderPid = plugReg.getPluginId(TransformPluginType.class, lm);

    transformMeta = new TransformMeta(loaderPid, "loader", lm);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipelineMeta.addTransform(transformMeta);

    loader = new PGBulkLoader(transformMeta, lm, ld, 1, pipelineMeta, pipeline);
  }

  @Test
  void testCheckWithMissingConnectionDoesNotAbortPipelineCheck() {
    // Regression for #9386: a connection name that is not in the metadata used to throw a
    // NullPointerException from check(), which failed the whole pipeline check.
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("missing-connection");
    pipelineMeta.setMetadataProvider(metadataProvider);

    PGBulkLoaderMeta meta = new PGBulkLoaderMeta();
    meta.setConnection("postgresql");
    meta.setTableName("target");
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    pipelineMeta.addTransform(new TransformMeta(pluginId, "loader", meta));

    List<ICheckResult> remarks = new ArrayList<>();
    assertDoesNotThrow(
        () ->
            pipelineMeta.checkTransforms(remarks, false, null, new Variables(), metadataProvider));

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    ReferencedDatabaseConnectionChecker.ERROR_DOES_NOT_EXIST.equals(
                        r.getErrorCode())));
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && BaseMessages.getString(
                                PGBulkLoaderMeta.class,
                                "GPBulkLoaderMeta.CheckResult.InvalidConnection")
                            .equals(r.getText())));
  }

  public static final class PGBulkLoaderMappingMetaValidator
      implements IFieldLoadSaveValidator<PGBulkLoaderMappingMeta> {

    @Override
    public PGBulkLoaderMappingMeta getTestObject() {
      return new PGBulkLoaderMappingMeta(
          UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(PGBulkLoaderMappingMeta testObject, Object actual) {
      return testObject.equals(actual);
    }
  }
}
