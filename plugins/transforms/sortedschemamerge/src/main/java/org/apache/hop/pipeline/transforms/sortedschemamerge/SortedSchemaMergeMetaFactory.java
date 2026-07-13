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
 *
 */

package org.apache.hop.pipeline.transforms.sortedschemamerge;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.util.Utils;

/** Builds {@link SortedSchemaMergeMeta} instances for generated pipelines. */
public final class SortedSchemaMergeMetaFactory {

  private SortedSchemaMergeMetaFactory() {}

  public static SortedSchemaMergeMeta create(
      List<String> inputTransformNames, List<SortedSchemaMergeSortKey> sortKeys) {
    SortedSchemaMergeMeta meta = new SortedSchemaMergeMeta();
    if (inputTransformNames != null) {
      for (String transformName : inputTransformNames) {
        if (!Utils.isEmpty(transformName)) {
          meta.getInputs().add(new SortedSchemaMergeInput(transformName));
        }
      }
    }
    if (sortKeys != null) {
      for (SortedSchemaMergeSortKey sortKey : sortKeys) {
        if (sortKey != null) {
          meta.getSortKeys().add(new SortedSchemaMergeSortKey(sortKey));
        }
      }
    }
    return meta;
  }

  public static List<SortedSchemaMergeSortKey> sortKeys(String... fieldNames) {
    List<SortedSchemaMergeSortKey> keys = new ArrayList<>();
    if (fieldNames == null) {
      return keys;
    }
    for (String fieldName : fieldNames) {
      if (!Utils.isEmpty(fieldName)) {
        keys.add(new SortedSchemaMergeSortKey(fieldName, true));
      }
    }
    return keys;
  }
}
