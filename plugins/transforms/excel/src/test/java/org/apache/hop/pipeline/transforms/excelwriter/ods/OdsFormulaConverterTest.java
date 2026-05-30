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

package org.apache.hop.pipeline.transforms.excelwriter.ods;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OdsFormulaConverterTest {

  @Test
  void convertsExcelFormulaPrefix() {
    assertEquals("of:=.[A1]+.[B2]", OdsFormulaConverter.toOdfFormula("=A1+B2"));
  }

  @Test
  void convertsSumRange() {
    assertEquals("of:=SUM([.A1:.A2])", OdsFormulaConverter.toOdfFormula("=SUM(A1:A2)"));
  }

  @Test
  void keepsOpenFormulaUntouched() {
    assertEquals("of:=.[A1]+.[A2]", OdsFormulaConverter.toOdfFormula("of:=.[A1]+.[A2]"));
  }

  @Test
  void convertsSheetReference() {
    assertEquals("of:=Data.[A1]+.[A2]", OdsFormulaConverter.toOdfFormula("=Data!A1+A2"));
  }
}
