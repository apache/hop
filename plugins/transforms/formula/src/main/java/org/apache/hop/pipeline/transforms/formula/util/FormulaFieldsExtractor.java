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
package org.apache.hop.pipeline.transforms.formula.util;

import java.util.ArrayList;
import java.util.List;

public final class FormulaFieldsExtractor {
  private FormulaFieldsExtractor() {
    // no-op
  }

  // avoid slow wildcard base regex compiled every time for such a simple logic
  public static List<String> getFormulaFieldList(final String formula) {
    List<String> theFields = new ArrayList<>();

    int from = 0;
    do {
      final int fieldStart = formula.indexOf('[', from);
      if (fieldStart < 0) {
        break;
      }
      final int fieldEnd = formula.indexOf(']', fieldStart);
      if (fieldEnd < 0) {
        from = fieldStart + 1;
      } else {
        from = fieldEnd + 1;
        if (fieldEnd > fieldStart - 1) {
          theFields.add(formula.substring(fieldStart + 1, fieldEnd));
        }
      }
    } while (from < formula.length() - 1);

    return theFields;
  }
}
