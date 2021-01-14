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

import org.apache.hop.core.IHopAttribute;
import org.apache.hop.core.row.IValueMeta;

/**
 * TODO: move this to XML somewhere...
 *
 * @author matt
 */
public enum SelectValuesAttr implements IHopAttribute {

  FOO( "fields", "CsvInputDialog.Fields.Label", null, IValueMeta.TYPE_NONE, null );

  /*
   * FIELDS("fields", "CsvInputDialog.Fields.Label", null, IValueMeta.TYPE_NONE, null), FIELD("field",
   * "CsvInputDialog.Field.Label", null, IValueMeta.TYPE_NONE, FIELDS), FIELD_NAME("name", "field_name",
   * "SelectValuesDialog.ColumnInfo.Fieldname", null, IValueMeta.TYPE_STRING, FIELD), FIELD_RENAME("rename",
   * "field_rename", "SelectValuesDialog.ColumnInfo.RenameTo", null, IValueMeta.TYPE_STRING, FIELD),
   * FIELD_LENGTH("length", "field_length", "SelectValuesDialog.ColumnInfo.Length", null,
   * IValueMeta.TYPE_STRING, FIELD), FIELD_PRECISION("precision", "field_precision",
   * "SelectValuesDialog.ColumnInfo.Precision", null, IValueMeta.TYPE_STRING, FIELD),
   *
   * SELECT_UNSPECIFIED("select_unspecified", "SelectValuesDialog.Unspecified.Label", null,
   * IValueMeta.TYPE_BOOLEAN, null),
   *
   * REMOVES("removes", "CsvInputDialog.Removes.Label", null, IValueMeta.TYPE_NONE, null), REMOVE("remove",
   * "CsvInputDialog.Remove.Label", null, IValueMeta.TYPE_NONE, REMOVES), REMOVE_NAME("name", "remove_name",
   * "SelectValuesDialog.ColumnInfo.Fieldname", null, IValueMeta.TYPE_STRING, REMOVE),
   *
   * METAS("metas", "CsvInputDialog.Metas.Label", null, IValueMeta.TYPE_NONE, null), META("meta",
   * "CsvInputDialog.Meta.Label", null, IValueMeta.TYPE_NONE, METAS), META_NAME("name", "meta_name",
   * "SelectValuesDialog.ColumnInfo.Fieldname", null, IValueMeta.TYPE_STRING, META), META_RENAME("rename",
   * "meta_rename", "SelectValuesDialog.ColumnInfo.Renameto", null, IValueMeta.TYPE_STRING, META),
   * META_TYPE("type", "meta_type", "SelectValuesDialog.ColumnInfo.Type", null, IValueMeta.TYPE_STRING, META),
   * META_LENGTH("length", "meta_length", "SelectValuesDialog.ColumnInfo.Length", null, IValueMeta.TYPE_STRING,
   * META), META_PRECISION("precision", "meta_precision", "SelectValuesDialog.ColumnInfo.Precision", null,
   * IValueMeta.TYPE_STRING, META), META_STORAGE_TYPE("storage_type", "meta_storage_type",
   * "SelectValuesDialog.ColumnInfo.Storage.Label", null, IValueMeta.TYPE_STRING, META),
   * META_CONVERSION_MASK("conversion_mask", "meta_conversion_mask", "SelectValuesDialog.ColumnInfo.Format", null,
   * IValueMeta.TYPE_STRING, META), META_ENCODING("encoding", "meta_encoding",
   * "SelectValuesDialog.ColumnInfo.Encoding", null, IValueMeta.TYPE_STRING, META),
   * META_DECIMAL("decimal_symbol", "meta_edecimal_symbol", "SelectValuesDialog.ColumnInfo.Decimal", null,
   * IValueMeta.TYPE_STRING, META), META_GROUPING("grouping_symbol", "meta_grouping_symbol",
   * "SelectValuesDialog.ColumnInfo.Grouping", null, IValueMeta.TYPE_STRING, META),
   * META_CURRENCY("currency_symbol", "meta_currency_symbol", "SelectValuesDialog.ColumnInfo.Currency", null,
   * IValueMeta.TYPE_STRING, META),
   */

  private String xmlCode;
  private String repCode;
  private String description;
  private String tooltip;
  private int type;
  private SelectValuesAttr parent;

  /**
   * @param xmlCode
   * @param repCode
   * @param description
   * @param tooltip
   * @param type
   */
  private SelectValuesAttr( String xmlCode, String repCode, String description, String tooltip, int type,
                            SelectValuesAttr parent ) {
    this.xmlCode = xmlCode;
    this.repCode = repCode;
    this.description = description;
    this.tooltip = tooltip;
    this.type = type;
    this.parent = parent;
  }

  /**
   * @param code
   * @param description
   * @param tooltip
   * @param type
   */
  private SelectValuesAttr( String code, String description, String tooltip, int type, SelectValuesAttr parent ) {
    this( code, code, description, tooltip, type, parent );
  }

  /**
   * @param xmlCode The XML Code to search for
   * @return the attribute that corresponds to the XML code or null if nothing was found.
   */
  public static SelectValuesAttr findByKey( String key ) {
    for ( SelectValuesAttr attr : values() ) {
      if ( attr.getKey().equals( key ) ) {
        return attr;
      }
    }
    return null;
  }

  public String getKey() {
    return repCode;
  }

  /**
   * @return the xmlCode
   */
  public String getXmlCode() {
    return xmlCode;
  }

  /**
   * @return the repCode
   */
  public String getRepCode() {
    return repCode;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return the tooltip
   */
  public String getTooltip() {
    return tooltip;
  }

  /**
   * @return the type
   */
  public int getType() {
    return type;
  }

  public SelectValuesAttr getParent() {
    return parent;
  }
}
