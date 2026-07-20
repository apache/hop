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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ValidatorMeta} */
class ValidatorMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    ValidatorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/validator-transform.xml", ValidatorMeta.class);

    assertTrue(meta.isValidatingAll());
    assertTrue(meta.isConcatenatingErrors());
    assertEquals(";", meta.getConcatenationSeparator());
    assertTrue(meta.isSuppressingLogFailedData());
    assertEquals(1, meta.getValidations().size());

    Validation validation = meta.getValidations().getFirst();
    assertEquals("value rules", validation.getName());
    assertEquals("value", validation.getFieldName());
    assertFalse(validation.isNullAllowed());
    assertEquals("10", validation.getMaximumLength());
    assertEquals("1", validation.getMinimumLength());
    assertEquals("CODE_NULL", validation.getErrorCode());
    assertEquals(2, validation.getAllowedValues().size());
    assertEquals("A", validation.getAllowedValues().get(0));
    assertEquals("B", validation.getAllowedValues().get(1));
  }

  @Test
  void testCloneCopiesOptionsAndValidations() {
    ValidatorMeta meta = new ValidatorMeta();
    meta.setValidatingAll(true);
    meta.setConcatenatingErrors(true);
    meta.setConcatenationSeparator("|");
    meta.setSuppressingLogFailedData(true);

    Validation validation = new Validation();
    validation.setName("rule1");
    validation.setFieldName("field1");
    validation.setNullAllowed(false);
    meta.getValidations().add(validation);

    ValidatorMeta clone = meta.clone();
    assertNotSame(meta, clone);
    assertNotSame(meta.getValidations(), clone.getValidations());
    assertNotSame(meta.getValidations().getFirst(), clone.getValidations().getFirst());
    assertTrue(clone.isValidatingAll());
    assertTrue(clone.isConcatenatingErrors());
    assertEquals("|", clone.getConcatenationSeparator());
    assertTrue(clone.isSuppressingLogFailedData());
    assertEquals(1, clone.getValidations().size());
    assertEquals("rule1", clone.getValidations().getFirst().getName());
    assertEquals("field1", clone.getValidations().getFirst().getFieldName());
  }

  @Test
  void testSupportsErrorHandling() {
    assertTrue(new ValidatorMeta().supportsErrorHandling());
  }

  @Test
  void testSuppressLogFailedDataDefaultsToFalse() {
    assertFalse(new ValidatorMeta().isSuppressingLogFailedData());
  }

  @Test
  void checkNoPrevFieldsAddsWarning() {
    ValidatorMeta meta = new ValidatorMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        null,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING));
  }

  @Test
  void checkEmptyPrevFieldsAddsWarning() {
    ValidatorMeta meta = new ValidatorMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING));
  }

  @Test
  void checkWithPrevAndInputAddsOkRemarks() {
    ValidatorMeta meta = new ValidatorMeta();
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("field1"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  @Test
  void checkNoInputAddsError() {
    ValidatorMeta meta = new ValidatorMeta();
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("field1"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void getTransformIOMetaCreatesInfoStreamPerValidation() {
    ValidatorMeta meta = new ValidatorMeta();
    Validation validation = new Validation();
    validation.setName("rule1");
    validation.setSourcingTransformName("source");
    meta.getValidations().add(validation);

    ITransformIOMeta ioMeta = meta.getTransformIOMeta();
    assertNotNull(ioMeta);
    assertEquals(1, ioMeta.getInfoStreams().size());
    assertEquals(1, meta.getTransformIOMeta().getInfoStreams().size());
  }

  @Test
  void searchInfoAndTargetTransformsResolvesSourcingTransform() {
    ValidatorMeta meta = new ValidatorMeta();
    Validation validation = new Validation();
    validation.setName("rule1");
    validation.setSourcingTransformName("source");
    meta.getValidations().add(validation);

    TransformMeta source = new TransformMeta();
    source.setName("source");
    meta.searchInfoAndTargetTransforms(List.of(source));

    assertSame(source, validation.getSourcingTransform());
  }

  @Test
  void searchInfoAndTargetTransformsClearsMissingSource() {
    ValidatorMeta meta = new ValidatorMeta();
    Validation validation = new Validation();
    validation.setName("rule1");
    validation.setSourcingTransformName("missing");
    meta.getValidations().add(validation);

    meta.searchInfoAndTargetTransforms(List.of());
    assertNull(validation.getSourcingTransform());
  }

  @Test
  void handleStreamSelectionAddsValidationForNewValidationStream() {
    ValidatorMeta meta = new ValidatorMeta();
    TransformMeta source = new TransformMeta();
    source.setName("allowed_values");

    IStream newValidation = ValidatorMeta.getNewValidation();
    newValidation.setTransformMeta(source);

    meta.handleStreamSelection(newValidation);

    assertEquals(1, meta.getValidations().size());
    Validation created = meta.getValidations().getFirst();
    assertEquals("allowed_values", created.getName());
    assertTrue(created.isSourcingValues());
    assertSame(source, created.getSourcingTransform());
  }

  @Test
  void handleStreamSelectionIgnoresUnrelatedStream() {
    ValidatorMeta meta = new ValidatorMeta();
    IStream other = mock(IStream.class);

    meta.handleStreamSelection(other);
    assertTrue(meta.getValidations().isEmpty());
  }

  @Test
  void newValidationStreamAccessorRoundTrip() {
    IStream original = ValidatorMeta.getNewValidation();
    assertNotNull(original);

    IStream replacement = mock(IStream.class);
    try {
      ValidatorMeta.setNewValidation(replacement);
      assertSame(replacement, ValidatorMeta.getNewValidation());
    } finally {
      ValidatorMeta.setNewValidation(original);
    }
    assertSame(original, ValidatorMeta.getNewValidation());
  }
}
