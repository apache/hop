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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.lineage.model.LineageFieldSchema;
import org.junit.jupiter.api.Test;

class LineageTransformSchemaEmitterTest {

  @Test
  void fieldsFromRowMeta_mapsNamesAndTypes() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("a"));
    rowMeta.addValueMeta(new ValueMetaInteger("count"));

    List<LineageFieldSchema> fields = LineageTransformSchemaEmitter.fieldsFromRowMeta(rowMeta);

    assertEquals(2, fields.size());
    assertEquals("a", fields.get(0).getName());
    assertEquals(
        ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING), fields.get(0).getHopTypeName());
    assertEquals("count", fields.get(1).getName());
    assertEquals(
        ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER), fields.get(1).getHopTypeName());
  }

  @Test
  void fieldsFromRowMeta_nullOrEmpty() {
    assertEquals(0, LineageTransformSchemaEmitter.fieldsFromRowMeta(null).size());
    assertEquals(0, LineageTransformSchemaEmitter.fieldsFromRowMeta(new RowMeta()).size());
  }
}
