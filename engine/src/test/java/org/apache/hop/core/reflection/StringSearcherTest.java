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
package org.apache.hop.core.reflection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
public class StringSearcherTest {

  /** A leaf metadata object holding a variable reference, e.g. a GetVariables field definition. */
  public static class Field {
    private String variableString;

    public Field(String variableString) {
      this.variableString = variableString;
    }

    public String getVariableString() {
      return variableString;
    }
  }

  /** Metadata shape used before the @HopMetadataProperty migration: an array of objects. */
  public static class ArrayHolder {
    private Field[] fields;

    public Field[] getFields() {
      return fields;
    }
  }

  /** Metadata shape used after the migration: a List of objects (issue #2481). */
  public static class ListHolder {
    private List<Field> fields;

    public List<Field> getFields() {
      return fields;
    }
  }

  /** A List of plain strings must also have its elements discovered. */
  public static class StringListHolder {
    private List<String> values;

    public List<String> getValues() {
      return values;
    }
  }

  private static boolean containsString(List<StringSearchResult> results, String expected) {
    return results.stream().anyMatch(r -> expected.equals(r.getString()));
  }

  @Test
  public void testFindMetaDataScansObjectArray() {
    ArrayHolder holder = new ArrayHolder();
    holder.fields = new Field[] {new Field("${VARIABLE_1}"), new Field("${VARIABLE_2}")};

    List<StringSearchResult> results = new ArrayList<>();
    StringSearcher.findMetaData(holder, 1, results, holder, holder);

    assertTrue(containsString(results, "${VARIABLE_1}"), "array element 1 not found");
    assertTrue(containsString(results, "${VARIABLE_2}"), "array element 2 not found");
  }

  /**
   * Regression test for <a href="https://github.com/apache/hop/issues/2481">#2481</a>: variables
   * nested in List-based metadata (the @HopMetadataProperty model most transforms migrated to) were
   * no longer discovered, so Run Options no longer pre-populated them.
   */
  @Test
  public void testFindMetaDataScansObjectList() {
    ListHolder holder = new ListHolder();
    holder.fields = new ArrayList<>();
    holder.fields.add(new Field("${VARIABLE_1}"));
    holder.fields.add(new Field("${VARIABLE_2}"));

    List<StringSearchResult> results = new ArrayList<>();
    StringSearcher.findMetaData(holder, 1, results, holder, holder);

    assertTrue(containsString(results, "${VARIABLE_1}"), "list element 1 not found");
    assertTrue(containsString(results, "${VARIABLE_2}"), "list element 2 not found");
  }

  @Test
  public void testFindMetaDataScansStringList() {
    StringListHolder holder = new StringListHolder();
    holder.values = new ArrayList<>();
    holder.values.add("${VARIABLE_1}");

    List<StringSearchResult> results = new ArrayList<>();
    StringSearcher.findMetaData(holder, 1, results, holder, holder);

    assertTrue(containsString(results, "${VARIABLE_1}"), "string list element not found");
  }
}
