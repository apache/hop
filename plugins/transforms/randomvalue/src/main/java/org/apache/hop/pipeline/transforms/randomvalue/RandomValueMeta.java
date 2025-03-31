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
package org.apache.hop.pipeline.transforms.randomvalue;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "RandomValue",
    image = "randomvalue.svg",
    name = "i18n::RandomValue.Name",
    description = "i18n::RandomValue.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::RandomValueMeta.keyword",
    documentationUrl = "/pipeline/transforms/generaterandomvalue.html")
@Getter
@Setter
public class RandomValueMeta extends BaseTransformMeta<RandomValue, RandomValueData> {
  private static final Class<?> PKG = RandomValueMeta.class;

  @HopMetadataProperty(key = "seed")
  private String seed;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<RVField> fields;

  public RandomValueMeta() {
    super();
    this.fields = new ArrayList<>();
  }

  public RandomValueMeta(RandomValueMeta m) {
    this();
    this.seed = m.seed;
    m.fields.forEach(f -> this.fields.add(new RVField(f)));
  }

  @Override
  public RandomValueMeta clone() {
    return new RandomValueMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    for (RVField field : fields) {
      IValueMeta v;

      switch (field.getType()) {
        case NUMBER:
          v = new ValueMetaNumber(field.getName(), 10, 5);
          break;
        case INTEGER:
          v = new ValueMetaInteger(field.getName(), 10, 0);
          break;
        case STRING:
          v = new ValueMetaString(field.getName(), 13, 0);
          break;
        case UUID, UUID4:
          v = new ValueMetaString(field.getName(), 36, 0);
          break;
        case HMAC_MD5, HMAC_SHA1, HMAC_SHA256, HMAC_SHA512, HMAC_SHA384:
          v = new ValueMetaString(field.getName(), 100, 0);
          break;
        default:
          v = new ValueMetaNone(field.getName());
          break;
      }
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    int nrRemarks = remarks.size();
    for (RVField field : fields) {
      if (field.getType() == null || field.getType() == RandomType.NONE) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "RandomValueMeta.CheckResult.FieldHasNoType", field.getName()),
                transformMeta);
        remarks.add(cr);
      }
    }
    if (remarks.size() == nrRemarks) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RandomValueMeta.CheckResult.AllTypesSpecified"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Getter
  public enum RandomType implements IEnumHasCodeAndDescription {
    NONE("", ""),
    NUMBER("random number", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomNumber")),
    INTEGER(
        "random integer", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomInteger")),
    STRING("random string", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomString")),
    UUID("random uuid", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomUUID")),
    UUID4("random uuid4", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomUUID4")),
    HMAC_MD5(
        "random machmacmd5", BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomHMACMD5")),
    HMAC_SHA1(
        "random machmacsha1",
        BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomHMACSHA1")),
    HMAC_SHA256(
        "random machmacsha256",
        BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomHMACSHA256")),
    HMAC_SHA512(
        "random machmacsha512",
        BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomHMACSHA512")),
    HMAC_SHA384(
        "random machmacsha384",
        BaseMessages.getString(PKG, "RandomValueMeta.TypeDesc.RandomHMACSHA384"));

    private final String code;

    private final String description;

    RandomType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(RandomType.class);
    }

    public static RandomType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(RandomType.class, description, NONE);
    }

    public static RandomType lookupCode(String code) {
      return IEnumHasCode.lookupCode(RandomType.class, code, NONE);
    }
  }

  @Getter
  @Setter
  public static final class RVField {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "type", storeWithCode = true)
    private RandomType type;

    public RVField() {
      type = RandomType.NONE;
    }

    public RVField(RVField f) {
      this();
      this.name = f.name;
      this.type = f.type;
    }
  }
}
