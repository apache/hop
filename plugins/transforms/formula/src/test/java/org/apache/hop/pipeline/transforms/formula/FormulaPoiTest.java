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

package org.apache.hop.pipeline.transforms.formula;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FormulaPoiTest {

  private FormulaPoi formulaPoi;

  @BeforeEach
  void setUp() throws IOException {
    formulaPoi = new FormulaPoi(msg -> System.out.println(msg)); // Provide a simple logger
  }

  @AfterEach
  void tearDown() throws IOException {
    if (formulaPoi != null) {
      formulaPoi.destroy();
    }
  }

  @Test
  void testConstructor() {
    assertNotNull(formulaPoi);
  }

  @Test
  void testEvaluatorCreation() {
    FormulaPoi.Evaluator evaluator = formulaPoi.evaluator(100);
    assertNotNull(evaluator);
    assertNotNull(evaluator.sheet());
    assertNotNull(evaluator.row());
    assertNotNull(evaluator.evaluator());
  }

  @Test
  void testEvaluatorForManyColumns() {
    FormulaPoi.Evaluator evaluator = formulaPoi.evaluator(500); // Forces XSS implementation
    assertNotNull(evaluator);
    assertNotNull(evaluator.sheet());
    assertNotNull(evaluator.row());
    assertNotNull(evaluator.evaluator());
  }

  @Test
  void testReset() {
    FormulaPoi.Evaluator evaluator = formulaPoi.evaluator(100);
    assertNotNull(evaluator);

    // Reset should clear any cached state
    formulaPoi.reset();

    // Should still work after reset
    FormulaPoi.Evaluator evaluator2 = formulaPoi.evaluator(100);
    assertNotNull(evaluator2);
  }

  @Test
  void testDestroy() throws IOException {
    formulaPoi.evaluator(100);
    formulaPoi.evaluator(500);

    // Should not throw exception
    formulaPoi.destroy();
  }
}
