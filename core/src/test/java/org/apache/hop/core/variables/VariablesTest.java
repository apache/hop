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

package org.apache.hop.core.variables;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Variables tests.
 *
 * @see Variables
 */
class VariablesTest {
  private final Variables variables = new Variables();

  /**
   * Checks if an ConcurrentModificationException while iterating over the System properties is
   * occurred.
   */
  @Test
  void initializeVariablesFrom() {
    final Variables variablesMock = mock(Variables.class);
    doCallRealMethod().when(variablesMock).initializeFrom(any(IVariables.class));

    final Map<String, String> propertiesMock = mock(Map.class);
    when(variablesMock.getProperties()).thenReturn(propertiesMock);

    doAnswer(
            new Answer<Map<String, String>>() {
              final String keyStub = "key";

              @Override
              public Map<String, String> answer(InvocationOnMock invocation) throws Throwable {
                if (System.getProperty(keyStub) == null) {
                  modifySystemProperties();
                }

                if (invocation.getArguments()[1] != null) {
                  propertiesMock.put(
                      (String) invocation.getArguments()[0],
                      System.getProperties().getProperty((String) invocation.getArguments()[1]));
                }
                return propertiesMock;
              }
            })
        .when(propertiesMock)
        .put(anyString(), anyString());

    variablesMock.initializeFrom(null);
    verify(variablesMock).initializeFrom(null);
  }

  private void modifySystemProperties() {
    final String keyStub = "key";
    final String valueStub = "value";

    Thread thread = new Thread(() -> System.setProperty(keyStub, valueStub));
    thread.start();
  }

  /** Spawns 20 threads that modify variables to test concurrent modification error fix. */
  @Test
  void testConcurrentModification() throws Exception {

    int threads = 20;
    List<Callable<Boolean>> callables = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      callables.add(newCallable());
    }

    // Assert threads ran successfully.
    for (Future<Boolean> result : Executors.newFixedThreadPool(5).invokeAll(callables)) {
      assertTrue(result.get());
    }
  }

  // Note:  Not using lambda so this can be ported to older version compatible with 1.7
  private Callable<Boolean> newCallable() {
    return () -> {
      for (int i = 0; i < 300; i++) {
        String key = "key" + i;
        variables.setVariable(key, "value");
        assertEquals("value", variables.resolve("${" + key + "}"));
      }
      return true;
    };
  }

  @Test
  void testFieldSubstitution() throws HopValueException {
    Object[] rowData = new Object[] {"DataOne", "DataTwo"};
    RowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("FieldOne"));
    rm.addValueMeta(new ValueMetaString("FieldTwo"));

    Variables vars = new Variables();
    assertNull(vars.resolve(null, rm, rowData));
    assertEquals("", vars.resolve("", rm, rowData));
    assertEquals("DataOne", vars.resolve("?{FieldOne}", rm, rowData));
    assertEquals("TheDataOne", vars.resolve("The?{FieldOne}", rm, rowData));
  }

  @Test
  void testEnvironmentSubstitute() {
    Variables vars = new Variables();
    vars.setVariable("VarOne", "DataOne");
    vars.setVariable("VarTwo", "DataTwo");

    assertNull(vars.resolve((String) null));
    assertEquals("", vars.resolve(""));
    assertEquals("DataTwo", vars.resolve("${VarTwo}"));
    assertEquals("DataTwoEnd", vars.resolve("${VarTwo}End"));

    assertEquals(0, vars.resolve(new String[0]).length);
    assertArrayEquals(
        new String[] {"DataOne", "TheDataOne"},
        vars.resolve(new String[] {"${VarOne}", "The${VarOne}"}));
  }

  /**
   * Null or empty variable names must not enter the properties map (issue #7067). A null key caused
   * NPE later when checking Const.INTERNAL_*_VARIABLES Set.of collections.
   */
  @Test
  void setVariableIgnoresNullAndEmptyNames() {
    Variables vars = new Variables();
    vars.setVariable(null, "shouldNotStore");
    vars.setVariable("", "shouldNotStore");
    vars.setVariable("valid", "ok");

    for (String name : vars.getVariableNames()) {
      assertTrue(name != null && !name.isEmpty());
    }
    assertEquals("ok", vars.getVariable("valid"));
    assertNull(vars.getVariable(null));
    assertNull(vars.getVariable(""));
  }

  /**
   * Transform variable spaces do not expose a metadata provider themselves; the running pipeline
   * (parent) does. Variable resolvers must walk that parent chain so remote export uses the bundled
   * metadata rather than the process-global store (#8096).
   */
  @Test
  void findExecutionMetadataProviderWalksParentChain() {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    IVariables parent =
        new Variables() {
          @Override
          public IHopMetadataProvider getMetadataProvider() {
            return provider;
          }
        };

    Variables child = new Variables();
    child.setParentVariables(parent);

    assertEquals(provider, child.findExecutionMetadataProvider());
    assertNull(new Variables().findExecutionMetadataProvider());
  }

  /**
   * An environment entry that nothing else defines, so that what it resolves to is unambiguous.
   * Names are sorted so the same entry is chosen on every run.
   */
  private static Map.Entry<String, String> environmentEntryNotDefinedElsewhere() {
    Set<String> definedNames = new HashSet<>(System.getProperties().stringPropertyNames());
    for (DescribedVariable describedVariable : HopConfig.getInstance().getDescribedVariables()) {
      definedNames.add(describedVariable.getName());
    }
    return System.getenv().entrySet().stream()
        .filter(entry -> !definedNames.contains(entry.getKey()))
        .filter(entry -> StringUtils.isNotEmpty(entry.getValue()))
        .min(Map.Entry.comparingByKey())
        .orElse(null);
  }

  /** The flag must not already be exported, or the process environment decides these tests. */
  private static void assumeFlagNotExported() {
    assumeTrue(
        System.getenv(Const.HOP_IMPORT_ENVIRONMENT_VARIABLES) == null,
        Const.HOP_IMPORT_ENVIRONMENT_VARIABLES + " is exported in this environment");
  }

  @AfterEach
  void clearEnvironmentImportFlag() {
    System.clearProperty(Const.HOP_IMPORT_ENVIRONMENT_VARIABLES);
  }

  /** The operating system environment stays out of the variable space unless it is asked for. */
  @Test
  void environmentIsNotImportedByDefault() {
    assumeFlagNotExported();
    Map.Entry<String, String> entry = environmentEntryNotDefinedElsewhere();
    assumeTrue(entry != null, "no usable environment variable to test with");

    Variables variables = new Variables();
    variables.initializeFrom(null);

    assertNull(variables.getVariable(entry.getKey()));
  }

  /** With the flag on, ${NAME} resolves an exported environment variable (#8495). */
  @Test
  void environmentIsImportedWhenEnabled() {
    assumeFlagNotExported();
    Map.Entry<String, String> entry = environmentEntryNotDefinedElsewhere();
    assumeTrue(entry != null, "no usable environment variable to test with");
    System.setProperty(Const.HOP_IMPORT_ENVIRONMENT_VARIABLES, "Y");

    Variables variables = new Variables();
    variables.initializeFrom(null);

    assertEquals(entry.getValue(), variables.getVariable(entry.getKey()));
    assertEquals(entry.getValue(), variables.resolve("${" + entry.getKey() + "}"));
  }

  /**
   * The environment has the lowest precedence, so a name that is also set with -D keeps the value
   * it resolves to today.
   */
  @Test
  void systemPropertiesWinOverTheEnvironment() {
    assumeFlagNotExported();
    Map.Entry<String, String> entry = environmentEntryNotDefinedElsewhere();
    assumeTrue(entry != null, "no usable environment variable to test with");
    assumeTrue(!"overridden-by-minus-D".equals(entry.getValue()));
    System.setProperty(Const.HOP_IMPORT_ENVIRONMENT_VARIABLES, "Y");
    System.setProperty(entry.getKey(), "overridden-by-minus-D");
    try {
      Variables variables = new Variables();
      variables.initializeFrom(null);

      assertEquals("overridden-by-minus-D", variables.getVariable(entry.getKey()));
    } finally {
      System.clearProperty(entry.getKey());
    }
  }

  /** Nothing set anywhere: off. */
  @Test
  void environmentImportIsOffWhenNothingIsSet() {
    assertFalse(Variables.isEnvironmentImported(null, null, null));
  }

  /**
   * The container case after HopEnvironment has started: it copied the default N from hop-config
   * onto the system properties, and the flag is exported as Y. The export must win (#8638).
   */
  @Test
  void exportedFlagWinsOverTheSeededDefault() {
    assertTrue(Variables.isEnvironmentImported("N", "Y", "N"));
  }

  /** Before HopEnvironment has started nothing is seeded, and an exported flag is enough. */
  @Test
  void exportedFlagIsEnoughBeforeHopEnvironmentStarts() {
    assertTrue(Variables.isEnvironmentImported(null, "Y", null));
  }

  /** HopEnvironment seeds an empty string when hop-config has the name without a value. */
  @Test
  void exportedFlagWinsOverASeededEmptyValue() {
    assertTrue(Variables.isEnvironmentImported(null, "Y", ""));
  }

  /** The environment overrides hop-config, as in HopResolvedSettings. */
  @Test
  void exportedFlagOverridesHopConfig() {
    assertFalse(Variables.isEnvironmentImported("Y", "N", "Y"));
    assertTrue(Variables.isEnvironmentImported("Y", null, "Y"));
  }

  /** A -D that differs from hop-config is an explicit override and wins over the environment. */
  @Test
  void minusDWinsOverTheEnvironment() {
    assertTrue(Variables.isEnvironmentImported("N", "N", "Y"));
    assertFalse(Variables.isEnvironmentImported("Y", "Y", "N"));
    assertTrue(Variables.isEnvironmentImported(null, null, "Y"));
  }
}
