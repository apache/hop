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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** Column delimiter declared on a Bulk API 2.0 ingest job. */
public enum SalesforceBulkDelimiter implements IEnumHasCodeAndDescription {
  COMMA("COMMA", ",", "SalesforceBulkDelimiter.Comma"),
  SEMICOLON("SEMICOLON", ";", "SalesforceBulkDelimiter.Semicolon"),
  TAB("TAB", "\t", "SalesforceBulkDelimiter.Tab"),
  PIPE("PIPE", "|", "SalesforceBulkDelimiter.Pipe"),
  CARET("CARET", "^", "SalesforceBulkDelimiter.Caret"),
  BACKQUOTE("BACKQUOTE", "`", "SalesforceBulkDelimiter.Backquote");

  private final String code;
  private final String character;
  private final String descriptionKey;

  SalesforceBulkDelimiter(String code, String character, String descriptionKey) {
    this.code = code;
    this.character = character;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  public String getCharacter() {
    return character;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(SalesforceBulkDelimiter.class, descriptionKey);
  }
}
