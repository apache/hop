/// *
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
package org.apache.hop.pipeline.transforms.selectvalues;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SelectValuesMetaInjectionTest extends BaseMetadataInjectionTestJunit5<SelectValuesMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
    selectValuesMeta
        .getSelectOption()
        .setSelectFields(SelectValueMetaTestFactory.getSelectFields("v", "v2"));
    setup(selectValuesMeta);
  }

  @Test
  @Disabled
  public void test() throws Exception {
    check(
        "SELECT_UNSPECIFIED",
        () -> meta.getSelectOption().isSelectingAndSortingUnspecifiedFields());
    check("FIELD_NAME", () -> meta.getSelectOption().getSelectFields().get(0).getName());
    check("FIELD_RENAME", () -> meta.getSelectOption().getSelectFields().get(0).getRename());
    check("FIELD_LENGTH", () -> meta.getSelectOption().getSelectFields().get(0).getLength());
    check("FIELD_PRECISION", () -> meta.getSelectOption().getSelectFields().get(0).getPrecision());
    check("REMOVE_NAME", () -> meta.getSelectOption().getDeleteName().get(0).getName());
    check("META_NAME", () -> meta.getSelectOption().getMeta().get(0).getName());
    check("META_RENAME", () -> meta.getSelectOption().getSelectFields().get(0).getRename());
    check("META_LENGTH", () -> meta.getSelectOption().getSelectFields().get(0).getLength());
    check("META_PRECISION", () -> meta.getSelectOption().getMeta().get(0).getPrecision());
    check(
        "META_CONVERSION_MASK", () -> meta.getSelectOption().getMeta().get(0).getConversionMask());
    check(
        "META_DATE_FORMAT_LENIENT",
        () -> meta.getSelectOption().getMeta().get(0).isDateFormatLenient());
    check(
        "META_DATE_FORMAT_LOCALE",
        () -> meta.getSelectOption().getMeta().get(0).getDateFormatLocale());
    check(
        "META_DATE_FORMAT_TIMEZONE",
        () -> meta.getSelectOption().getMeta().get(0).getDateFormatTimeZone());
    check(
        "META_LENIENT_STRING_TO_NUMBER",
        () -> meta.getSelectOption().getMeta().get(0).isLenientStringToNumber());
    check("META_DECIMAL", () -> meta.getSelectOption().getMeta().get(0).getDecimalSymbol());
    check("META_GROUPING", () -> meta.getSelectOption().getMeta().get(0).getGroupingSymbol());
    check("META_CURRENCY", () -> meta.getSelectOption().getMeta().get(0).getCurrencySymbol());
    check("META_ENCODING", () -> meta.getSelectOption().getMeta().get(0).getEncoding());

    IValueMeta mftt = new ValueMetaString("f");
    injector.setProperty(meta, "META_STORAGE_TYPE", setValue(mftt, "normal"), "f");
    assertEquals(
        ValueMetaFactory.getValueMetaName(0),
        meta.getSelectOption().getMeta().get(0).getStorageType());
    injector.setProperty(meta, "META_STORAGE_TYPE", setValue(mftt, "binary-string"), "f");
    assertEquals(
        ValueMetaFactory.getValueMetaName(1),
        meta.getSelectOption().getMeta().get(0).getStorageType());
    injector.setProperty(meta, "META_STORAGE_TYPE", setValue(mftt, "indexed"), "f");
    assertEquals(
        ValueMetaFactory.getValueMetaName(2),
        meta.getSelectOption().getMeta().get(0).getStorageType());
    skipPropertyTest("META_STORAGE_TYPE");

    // TODO check field type plugins
    skipPropertyTest("META_TYPE");
  }

  // test default values length and precision after injection
  @Test
  void testDefaultValue() throws Exception {
    IValueMeta valueMeta = new ValueMetaString("f");
    injector.setProperty(meta, "FIELD_NAME", setValue(valueMeta, "testValue"), "f");
    nonTestedProperties.clear(); // we don't need to test other properties
    assertEquals(
        SelectValuesMeta.UNDEFINED, meta.getSelectOption().getSelectFields().get(0).getLength());
    assertEquals(
        SelectValuesMeta.UNDEFINED, meta.getSelectOption().getSelectFields().get(0).getPrecision());
  }
}
