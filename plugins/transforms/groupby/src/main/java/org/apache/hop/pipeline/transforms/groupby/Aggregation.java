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
 *
 */

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class Aggregation implements Cloneable {

  private static final Class<?> PKG = Aggregation.class; // For Translator

  public static final int TYPE_GROUP_NONE = 0;

  public static final int TYPE_GROUP_SUM = 1;

  public static final int TYPE_GROUP_AVERAGE = 2;

  public static final int TYPE_GROUP_MEDIAN = 3;

  public static final int TYPE_GROUP_PERCENTILE = 4;

  public static final int TYPE_GROUP_MIN = 5;

  public static final int TYPE_GROUP_MAX = 6;

  public static final int TYPE_GROUP_COUNT_ALL = 7;

  public static final int TYPE_GROUP_CONCAT_COMMA = 8;

  public static final int TYPE_GROUP_FIRST = 9;

  public static final int TYPE_GROUP_LAST = 10;

  public static final int TYPE_GROUP_FIRST_INCL_NULL = 11;

  public static final int TYPE_GROUP_LAST_INCL_NULL = 12;

  public static final int TYPE_GROUP_CUMULATIVE_SUM = 13;

  public static final int TYPE_GROUP_CUMULATIVE_AVERAGE = 14;

  public static final int TYPE_GROUP_STANDARD_DEVIATION = 15;

  public static final int TYPE_GROUP_CONCAT_STRING = 16;

  public static final int TYPE_GROUP_COUNT_DISTINCT = 17;

  public static final int TYPE_GROUP_COUNT_ANY = 18;

  public static final int TYPE_GROUP_STANDARD_DEVIATION_SAMPLE = 19;

  public static final int TYPE_GROUP_PERCENTILE_NEAREST_RANK = 20;

  public static final int TYPE_GROUP_CONCAT_STRING_CRLF = 21;

  public static final String[]
          typeGroupLabel = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */ {
          "-",
          "SUM",
          "AVERAGE",
          "MEDIAN",
          "PERCENTILE",
          "MIN",
          "MAX",
          "COUNT_ALL",
          "CONCAT_COMMA",
          "FIRST",
          "LAST",
          "FIRST_INCL_NULL",
          "LAST_INCL_NULL",
          "CUM_SUM",
          "CUM_AVG",
          "STD_DEV",
          "CONCAT_STRING",
          "COUNT_DISTINCT",
          "COUNT_ANY",
          "STD_DEV_SAMPLE",
          "PERCENTILE_NEAREST_RANK",
          "CONCAT_STRING_CRLF"
  };

  public static final String[] typeGroupLongDesc = {
          "-",
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.SUM"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.MIN"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.MAX"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_ALL"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.FIRST"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.LAST"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_SUM"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_AVERAGE"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ANY"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK"),
          BaseMessages.getString(PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING_CRLF")
  };


  @HopMetadataProperty(key ="aggregate", injectionKey = "AGG_FIELD", injectionKeyDescription = "GroupByMeta.Injection.AGG_FIELD")
  private String field;

  @HopMetadataProperty(injectionKey = "AGG_SUBJECT", injectionKeyDescription = "GroupByMeta.Injection.AGG_SUBJECT")
  private String subject;

  @HopMetadataProperty(key = "type", injectionKey = "AGG_TYPE", injectionKeyDescription = "GroupByMeta.Injection.AGG_TYPE")
  private String typeLabel;

  private int type;

  @HopMetadataProperty(key ="valuefield", injectionKey = "AGG_VALUE", injectionKeyDescription = "GroupByMeta.Injection.AGG_VALUE")
  private String value;

  public Aggregation() {}

  public Aggregation(String field, String subject, String typeDesc, String value) {
    this.field = field;
    this.subject = subject;
    this.typeLabel = getTypeLabelFromLongDesc(typeDesc);
    this.value = value;
    this.type = getTypeCodeFromLongDesc(typeDesc);
  }

  @Override
  public Aggregation clone() {
    return new Aggregation(field, subject, typeLabel, value);
  }

  /**
   * Gets field
   *
   * @return value of field
   */
  public String getField() {
    return field;
  }

  /** @param field The field to set */
  public void setField(String field) {
    this.field = field;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public String getSubject() {
    return subject;
  }

  /** @param subject The subject to set */
  public void setSubject(String subject) {
    this.subject = subject;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getTypeLabel() {
    return typeLabel;
  }


  public void setTypeLabel(String typeCode) {
    this.typeLabel = typeCode;
    this.type = getTypeCodeFromLabel(typeCode);
  }

  /**
   * Gets value
   *
   * @return value of value
   */
  public String getValue() {
    return value;
  }

  /** @param value The value to set */
  public void setValue(String value) {
    this.value = value;
  }

  public int getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(int type) {
    this.type = type;
  }


  public static final int getTypeCodeFromLongDesc(String desc) {
    for (int i = 0; i < typeGroupLongDesc.length; i++) {
      if (typeGroupLongDesc[i].equalsIgnoreCase(desc)) {
        return i;
      }
    }
    return 0;
  }

  public static final int getTypeCodeFromLabel(String label) {
    for (int i = 0; i < typeGroupLabel.length; i++) {
      if (typeGroupLabel[i].equalsIgnoreCase(label)) {
        return i;
      }
    }
    return 0;
  }

  public static final String getTypeLabelFromLongDesc(String desc) {
    int descPos = 0;
    for (int i = 0; i < typeGroupLongDesc.length; i++) {
      if (typeGroupLongDesc[i].equalsIgnoreCase(desc)) {
        descPos = i;
      }
    }
    return typeGroupLabel[descPos];
  }

  public static final String getTypeLabelFromCode(int i) {
    if (i < 0 || i >= typeGroupLabel.length) {
      return null;
    }
    return typeGroupLabel[i];
  }

  public static final String getTypeDescLongFromCode(int i) {
    if (i < 0 || i >= typeGroupLongDesc.length) {
      return null;
    }
    return typeGroupLongDesc[i];
  }

}
