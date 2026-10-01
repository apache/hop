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
package org.apache.hop.pipeline.transforms.selectvalues;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/**
 * What Verify says about a Select Values transform.
 *
 * <p>The three tabs run in order, each on the row the one before it produced, and the checks used
 * to compare all of them against the incoming row. A field renamed on "Select &amp; Alter" was
 * therefore reported as missing on the tabs that only ever see its new name.
 *
 * @see <a href="https://github.com/apache/hop/issues/8294">#8294</a>
 */
public class SelectValuesMetaCheckTest {

  /** The transform from the issue: rename a field, then set the metadata on the new name. */
  @Test
  public void aFieldRenamedOnTheSelectTabIsFoundByTheMetadataTab() {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption()
        .setSelectFields(new ArrayList<>(List.of(select("value", "valueToSqrt"))));
    meta.getSelectOption().setMeta(new ArrayList<>(List.of(metadataChange("valueToSqrt"))));

    assertNoProblem(check(meta, rowOf("value")));
  }

  /** The Remove tab runs after the rename too, so it sees the new name as well. */
  @Test
  public void aFieldRenamedOnTheSelectTabIsFoundByTheRemoveTab() {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption()
        .setSelectFields(new ArrayList<>(List.of(select("value", "valueToSqrt"), select("id"))));
    DeleteField delete = new DeleteField();
    delete.setName("valueToSqrt");
    meta.getSelectOption().setDeleteName(new ArrayList<>(List.of(delete)));

    assertNoProblem(check(meta, rowOf("value", "id")));
  }

  /** Naming the same field twice is how a value is copied under a second name. */
  @Test
  public void selectingTheSameFieldTwiceUnderDifferentNamesIsNotAProblem() {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption()
        .setSelectFields(new ArrayList<>(List.of(select("value", "valueToSqrt"), select("value"))));

    assertNoProblem(check(meta, rowOf("value")));
  }

  /** What downstream transforms genuinely cannot deal with: two fields under one name. */
  @Test
  public void twoFieldsLeavingUnderTheSameNameIsStillReported() {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption()
        .setSelectFields(
            new ArrayList<>(List.of(select("value", "amount"), select("total", "amount"))));

    assertTrue(
        problems(check(meta, rowOf("value", "total"))).stream()
            .anyMatch(text -> text.contains("amount")),
        "two fields renamed to 'amount' is a real problem");
  }

  /** A field that really is absent is still reported, on every tab. */
  @Test
  public void afieldThatIsNowhereIsStillReported() {
    SelectValuesMeta meta = new SelectValuesMeta();
    meta.getSelectOption().setSelectFields(new ArrayList<>(List.of(select("value"))));
    meta.getSelectOption().setMeta(new ArrayList<>(List.of(metadataChange("noSuchField"))));

    assertTrue(
        problems(check(meta, rowOf("value"))).stream()
            .anyMatch(text -> text.contains("noSuchField")),
        "a metadata change on a field that does not exist is a real problem");
  }

  private static List<ICheckResult> check(SelectValuesMeta meta, IRowMeta previousRow) {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        null,
        new TransformMeta("SelectValues", "valueToSqrt", meta),
        previousRow,
        new String[] {"input"},
        null,
        null,
        null,
        null);
    return remarks;
  }

  private static List<String> problems(List<ICheckResult> remarks) {
    List<String> texts = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() != ICheckResult.TYPE_RESULT_OK) {
        texts.add(remark.getText());
      }
    }
    return texts;
  }

  private static void assertNoProblem(List<ICheckResult> remarks) {
    List<String> problems = problems(remarks);
    assertTrue(problems.isEmpty(), "nothing is wrong with this transform: " + problems);
  }

  private static IRowMeta rowOf(String... names) {
    IRowMeta row = new RowMeta();
    for (String name : names) {
      row.addValueMeta(
          "value".equals(name) ? new ValueMetaNumber(name) : new ValueMetaString(name));
    }
    return row;
  }

  private static SelectField select(String name) {
    return select(name, null);
  }

  private static SelectField select(String name, String rename) {
    SelectField field = new SelectField();
    field.setName(name);
    field.setRename(rename);
    return field;
  }

  private static SelectMetadataChange metadataChange(String name) {
    SelectMetadataChange change = new SelectMetadataChange();
    change.setName(name);
    return change;
  }
}
