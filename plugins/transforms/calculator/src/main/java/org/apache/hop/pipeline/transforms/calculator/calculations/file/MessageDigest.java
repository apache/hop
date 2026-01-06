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

package org.apache.hop.pipeline.transforms.calculator.calculations.file;

import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.calculator.CalculationInput;
import org.apache.hop.pipeline.transforms.calculator.CalculationOutput;
import org.apache.hop.pipeline.transforms.calculator.ICalculation;

public class MessageDigest implements ICalculation {
  private final String code;
  private final String algorithmType;
  private final String description;

  public MessageDigest(ALGORITHM algorithm) {
    this.code = algorithm.name();
    this.algorithmType = algorithm.type;
    this.description = BaseMessages.getString(PKG, algorithm.messageKey);
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public int getDefaultResultType() {
    return IValueMeta.TYPE_STRING;
  }

  @Override
  public CalculationOutput calculate(CalculationInput in) throws HopFileNotFoundException {
    return new CalculationOutput(
        getDefaultResultType(),
        ValueDataUtil.createChecksum(in.dataA, algorithmType, in.isFailIfNoFile));
  }

  public enum ALGORITHM {
    MD5("MD5", "CalculatorMetaFunction.CalcFunctions.MD5"),
    SHA1("SHA-1", "CalculatorMetaFunction.CalcFunctions.SHA1"),
    SHA256("SHA-256", "CalculatorMetaFunction.CalcFunctions.SHA256"),
    SHA384("SHA-384", "CalculatorMetaFunction.CalcFunctions.SHA384"),
    SHA512("SHA-512", "CalculatorMetaFunction.CalcFunctions.SHA512");
    final String type, messageKey;

    ALGORITHM(String type, String messageKey) {
      this.type = type;
      this.messageKey = messageKey;
    }
  }
}
