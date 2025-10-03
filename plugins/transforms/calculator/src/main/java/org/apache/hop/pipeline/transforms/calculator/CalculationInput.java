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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;

public class CalculationInput {
  public final IValueMeta metaA, metaB, metaC;
  public final Object dataA, dataB, dataC;
  public final CalculatorMetaFunction calcMetaFn;
  public final IValueMeta targetMeta;
  public final IVariables variables;
  public final boolean isFailIfNoFile;

  public CalculationInput(
      IValueMeta metaA,
      IValueMeta metaB,
      IValueMeta metaC,
      Object dataA,
      Object dataB,
      Object dataC,
      CalculatorMetaFunction calcMetaFn,
      IValueMeta targetMeta,
      IVariables variables,
      boolean isFailIfNoFile) {
    this.metaA = metaA;
    this.metaB = metaB;
    this.metaC = metaC;
    this.dataA = dataA;
    this.dataB = dataB;
    this.dataC = dataC;
    this.calcMetaFn = calcMetaFn;
    this.targetMeta = targetMeta;
    this.variables = variables;
    this.isFailIfNoFile = isFailIfNoFile;
  }
}
