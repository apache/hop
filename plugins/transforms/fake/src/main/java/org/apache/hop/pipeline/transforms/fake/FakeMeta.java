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

package org.apache.hop.pipeline.transforms.fake;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "Fake",
    image = "fake.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Fake",
    description = "i18n::BaseTransform.TypeTooltipDesc.Fake",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = {"fake", "data", "generator", "synthetic"}, // TODO : i18n
    documentationUrl = "/pipeline/transforms/fake.html")
public class FakeMeta extends BaseTransformMeta<Fake, FakeData> {

  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Locale")
  private String locale;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "Fake.Injection.Fields")
  private List<FakeField> fields;

  public FakeMeta() {
    super(); // allocate BaseTransformMeta
    this.fields = new ArrayList<>();
  }

  @Override
  public FakeMeta clone() {
    FakeMeta copy = (FakeMeta) super.clone();
    copy.locale = locale;
    copy.fields = new ArrayList<>();
    for (FakeField field : fields) {
      copy.fields.add(new FakeField(field));
    }
    return copy;
  }

  @Override
  public void setDefault() {
    locale = "en";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (FakeField field : fields) {
      if (field.isValid()) {
        IValueMeta v = new ValueMetaString(field.getName());
        v.setOrigin(name);
        rowMeta.addValueMeta(v);
      }
    }
  }

  public static final String[] getFakerLocales() {
    return new String[] {
      "ar",
      "bg",
      "by",
      "ca",
      "ca-CAT",
      "da-DK",
      "de",
      "de-AT",
      "de-CH",
      "ee",
      "en",
      "en-AU",
      "en-au-ocker",
      "en-BORK",
      "en-CA",
      "en-GB",
      "en-IND",
      "en-MS",
      "en-NEP",
      "en-NG",
      "en-NZ",
      "en-PAK",
      "en-SG",
      "en-UG",
      "en-US",
      "en-ZA",
      "es",
      "es-MX",
      "fa",
      "fi-FI",
      "fr",
      "fr-CA",
      "fr-CH",
      "he",
      "hu",
      "hy",
      "id",
      "in-ID",
      "it",
      "ja",
      "ko",
      "lv",
      "nb-NO",
      "nl",
      "no-NO",
      "pl",
      "pt",
      "pt-BR",
      "ru",
      "sk",
      "sv",
      "sv-SE",
      "th",
      "tr",
      "uk",
      "vi",
      "zh-CN",
      "zh-TW",
    };
  }

  /**
   * Gets locale
   *
   * @return value of locale
   */
  public String getLocale() {
    return locale;
  }

  /** @param locale The locale to set */
  public void setLocale(String locale) {
    this.locale = locale;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<FakeField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<FakeField> fields) {
    this.fields = fields;
  }
}
