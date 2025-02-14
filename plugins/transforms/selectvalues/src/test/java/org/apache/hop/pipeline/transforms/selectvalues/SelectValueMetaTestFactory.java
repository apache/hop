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

package org.apache.hop.pipeline.transforms.selectvalues;

import java.util.ArrayList;
import java.util.List;

public class SelectValueMetaTestFactory {

  public static List<SelectField> getSelectFields(String... fields) {
    List<SelectField> result = new ArrayList<>();
    for (String field : fields) {
      if (field == null) {
        throw new IllegalArgumentException("Field name cannot be null");
      }
      SelectField selectField = new SelectField();
      selectField.setName(field);
      result.add(selectField);
    }
    return result;
  }

  public static List<SelectField> getSelectFieldsWithRename(
      List<String> names, List<String> renames) {
    List<SelectField> result = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      SelectField selectField = new SelectField();
      selectField.setName(names.get(i));
      selectField.setRename(renames.get(i));
      result.add(selectField);
    }
    return result;
  }
}
