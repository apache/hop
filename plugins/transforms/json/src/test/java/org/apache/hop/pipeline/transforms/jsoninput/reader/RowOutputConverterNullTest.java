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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.BitSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RowOutputConverterNullTest {
  @ParameterizedTest
  @ValueSource(strings = {"Y", "N"})
  void jsonNullIsNotConvertedIntoAnEmptyString(String distinguish) throws Exception {
    String before = System.getProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL);
    try {
      System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, distinguish);
      JsonInputData data = new JsonInputData();
      data.outputRowMeta = new RowMeta();
      data.outputRowMeta.addValueMeta(new ValueMetaString("nullValue"));
      data.outputRowMeta.addValueMeta(new ValueMetaString("emptyValue"));
      data.convertRowMeta = data.outputRowMeta.cloneToType(ValueMetaString.TYPE_STRING);
      data.totalpreviousfields = 0;
      data.repeatedFields = new BitSet();
      RowOutputConverter converter = new RowOutputConverter(mock(ILogChannel.class));
      Object[] row =
          converter.getRow(
              new Object[0], new Object[] {NullNode.getInstance(), TextNode.valueOf("")}, data);
      assertNull(row[0]);
      assertEquals("", row[1]);
      if ("Y".equals(distinguish)) {
        assertTrue(!data.outputRowMeta.getValueMeta(1).isNull(row[1]));
      }
    } finally {
      if (before == null) {
        System.clearProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL);
      } else {
        System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, before);
      }
    }
  }
}
