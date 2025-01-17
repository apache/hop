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

package org.apache.hop.staticschema.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SchemaFieldDefinition implements Serializable {

  @HopMetadataProperty private String name;

  @HopMetadataProperty private String hopType;

  @HopMetadataProperty private int length;

  @HopMetadataProperty private int precision;

  @HopMetadataProperty private String formatMask;

  @HopMetadataProperty private String currencySymbol;

  @HopMetadataProperty private String decimalSymbol;

  @HopMetadataProperty private String groupingSymbol;

  @HopMetadataProperty private int trimType;

  @HopMetadataProperty private String ifNullValue;

  @HopMetadataProperty private String comment;

  @HopMetadataProperty private String roundingType;

  public SchemaFieldDefinition() {}

  public SchemaFieldDefinition(String name, String hopType) {
    this.name = name;
    this.hopType = hopType;
  }

  public IValueMeta getValueMeta() throws HopPluginException {
    int type = ValueMetaFactory.getIdForValueMeta(hopType);
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);

    if (valueMeta.getType() == IValueMeta.TYPE_NUMBER
        || valueMeta.getType() == IValueMeta.TYPE_INTEGER
        || valueMeta.getType() == IValueMeta.TYPE_DATE
        || valueMeta.getType() == IValueMeta.TYPE_BIGNUMBER
        || valueMeta.getType() == IValueMeta.TYPE_TIMESTAMP) {

      valueMeta.setConversionMask(formatMask);
      valueMeta.setRoundingType(roundingType);
    }

    if (valueMeta.getType() == IValueMeta.TYPE_NUMBER
        || valueMeta.getType() == IValueMeta.TYPE_BIGNUMBER) {
      valueMeta.setDecimalSymbol(decimalSymbol);
      valueMeta.setGroupingSymbol(groupingSymbol);
      valueMeta.setCurrencySymbol(currencySymbol);
    }

    valueMeta.setTrimType(trimType);
    valueMeta.setComments(comment);

    return valueMeta;
  }
}
