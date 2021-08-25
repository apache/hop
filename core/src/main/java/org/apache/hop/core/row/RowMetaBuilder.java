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
 */

package org.apache.hop.core.row;

import org.apache.hop.core.row.value.*;

public class RowMetaBuilder {

  private RowMeta rowMeta;

  public RowMetaBuilder() {
    this.rowMeta = new RowMeta();
  }

  public RowMetaBuilder addString(String name) {
    return addString(name, -1);
  }

  public RowMetaBuilder addString(String name, int length) {
    rowMeta.addValueMeta(new ValueMetaString(name, length, -1));
    return this;
  }

  public RowMetaBuilder addInteger(String name) {
    return addInteger(name, -1);
  }

  public RowMetaBuilder addInteger(String name, int length) {
    rowMeta.addValueMeta(new ValueMetaInteger(name, length, -1));
    return this;
  }

  public RowMetaBuilder addNumber(String name) {
    return addNumber(name, -1, -1);
  }

  public RowMetaBuilder addNumber(String name, int length, int precision) {
    rowMeta.addValueMeta(new ValueMetaNumber(name, length, -1));
    return this;
  }

  public RowMetaBuilder addBigNumber(String name, int length, int precision) {
    rowMeta.addValueMeta(new ValueMetaBigNumber(name, length, -1));
    return this;
  }

  public RowMetaBuilder addDate(String name) {
    rowMeta.addValueMeta(new ValueMetaDate(name));
    return this;
  }

  public RowMetaBuilder addBoolean(String name) {
    rowMeta.addValueMeta(new ValueMetaBoolean(name));
    return this;
  }

  public RowMetaBuilder remove(int index) {
    rowMeta.removeValueMeta(index);
    return this;
  }

  public IRowMeta build() {
    return rowMeta;
  }
}
