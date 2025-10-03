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

import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

public interface ICalculation {
  Class<?> PKG = ICalculation.class;

  /**
   * @return A unique code for lookup. This code is referenced from pipeline/workflow files.
   */
  String getCode();

  /**
   * @return An i18n description that is shown in the UI.
   */
  String getDescription();

  /**
   * @return the default {@link org.apache.hop.core.row.IValueMeta} value type, if it cannot or
   *     should not be derived from the calculation input
   */
  int getDefaultResultType();

  CalculationOutput calculate(CalculationInput i)
      throws HopValueException, HopFileNotFoundException;

  static int getResultType(IValueMeta valueMeta) {
    return valueMeta == null ? IValueMeta.TYPE_NONE : valueMeta.getType();
  }
}
