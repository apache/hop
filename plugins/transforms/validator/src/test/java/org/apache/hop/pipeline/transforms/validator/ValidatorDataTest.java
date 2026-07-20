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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.pipeline.transform.ITransformData;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ValidatorData}. */
class ValidatorDataTest {

  @Test
  void constructorCreatesUsableTransformData() {
    ValidatorData data = new ValidatorData();

    assertNotNull(data);
    assertInstanceOf(ITransformData.class, data);
    assertNull(data.fieldIndexes);
    assertNull(data.constantsMeta);
    assertNull(data.inputRowMeta);
    assertNull(data.patternExpected);
    assertNull(data.patternDisallowed);
  }
}
