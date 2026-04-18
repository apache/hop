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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UserDefinedJavaClass} lifecycle: constructor, {@code processRow()}, and
 * {@code init()}.
 */
class UserDefinedJavaClassTest {

  private TransformMockHelper<UserDefinedJavaClassMeta, UserDefinedJavaClassData> helper;

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        ValueMetaString.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    registry.registerPluginClass(
        ValueMetaInteger.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "UDJC TEST", UserDefinedJavaClassMeta.class, UserDefinedJavaClassData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  // ------------------------------------------------------------------ helpers

  /**
   * Builds a {@link UserDefinedJavaClass} backed by the given meta. Uses {@code copyNr=0} so the
   * constructor calls {@code cookClasses()}.
   */
  private UserDefinedJavaClass build(UserDefinedJavaClassMeta meta) {
    return new UserDefinedJavaClass(
        helper.transformMeta,
        meta,
        new UserDefinedJavaClassData(),
        0,
        helper.pipelineMeta,
        helper.pipeline);
  }

  // ------------------------------------------------------------------ processRow: null child

  @Test
  void processRow_withNullChild_returnsFalse() throws Exception {
    // Meta with no definitions → no transform class → child stays null
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    UserDefinedJavaClass transform = build(meta);

    assertNull(transform.getChild());
    assertFalse(transform.processRow());
  }

  // ------------------------------------------------------------------ init: no cooked class

  @Test
  void init_noTransformClassDefined_returnsFalse() {
    // Meta with no definitions → cookedTransformClass remains null
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    UserDefinedJavaClass transform = build(meta);

    assertFalse(transform.init());
  }

  // ------------------------------------------------------------------ init: cook errors

  @Test
  void init_withCookErrors_returnsFalse() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    // Duplicate method → compilation error
    String badSource =
        """
        public boolean processRow() { return true; }
        public boolean processRow() { return true; }
        """;
    meta.getDefinitions()
        .add(new UserDefinedJavaClassDef(ClassType.NORMAL_CLASS, "BadClass", badSource));

    UserDefinedJavaClass transform = build(meta);

    assertFalse(meta.getCookErrors().isEmpty());
    assertFalse(transform.init());
  }

  // ------------------------------------------------------------------ constructor: copyNr != 0
  // skips explicit cook

  @Test
  void constructor_copyNrNonZero_constructsWithoutThrowingAndNoCookErrors() {
    /*
     * Note:
     * For copyNr=1, the explicit cookClasses() call in the constructor body is skipped.
     * Cooking is performed lazily inside newChildInstance() via checkClassCooked().
     *
     * For a clean meta with no definitions, no cook errors will occur.
     */
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();

    new UserDefinedJavaClass(
        helper.transformMeta,
        meta,
        new UserDefinedJavaClassData(),
        1,
        helper.pipelineMeta,
        helper.pipeline);

    assertTrue(meta.getCookErrors().isEmpty());
  }

  // ------------------------------------------------------------------ cookClasses: TRANSFORM_CLASS

  @Test
  void cookClasses_withValidTransformClass_setsCookedClass() throws Exception {
    // Call cookClasses() directly to verify compilation independent of child instantiation
    // (child instantiation fails in tests because pipelineMeta.getPrevTransformFields returns
    // null).
    String source =
        """
        public boolean processRow() throws org.apache.hop.core.exception.HopException {
          setOutputDone();
          return false;
        }
        """;

    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.getDefinitions()
        .add(new UserDefinedJavaClassDef(ClassType.TRANSFORM_CLASS, "Processor", source));

    meta.cookClasses();

    assertTrue(meta.getCookErrors().isEmpty(), "Expected no cook errors");
    assertNotNull(meta.getCookedTransformClass(), "Expected cookedTransformClass to be set");
  }

  // ------------------------------------------------------------------ getDefinitions / hasChanged

  @Test
  void metaGetDefinitions_initiallyEmpty() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    assertTrue(meta.getDefinitions().isEmpty());
  }

  @Test
  void metaReplaceDefinitions_setsHasChanged() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.replaceDefinitions(Collections.emptyList());
    assertTrue(meta.isHasChanged());
  }
}
