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

package org.apache.hop.openformula;

import org.apache.hop.openformula.error.FormulaErrorValue;

/** Thrown when formula evaluation encounters a runtime evaluation error. */
public class EvaluationException extends Exception {
  private static final long serialVersionUID = 1L;

  private final FormulaErrorValue errorValue;

  public EvaluationException(FormulaErrorValue errorValue) {
    super(errorValue != null ? errorValue.toString() : "Evaluation error");
    this.errorValue = errorValue != null ? errorValue : FormulaErrorValue.ERROR_UNEXPECTED_VALUE;
  }

  public EvaluationException(FormulaErrorValue errorValue, Throwable cause) {
    super(errorValue != null ? errorValue.toString() : "Evaluation error", cause);
    this.errorValue = errorValue != null ? errorValue : FormulaErrorValue.ERROR_UNEXPECTED_VALUE;
  }

  public EvaluationException(int errorCode) {
    this(new FormulaErrorValue(errorCode));
  }

  public static EvaluationException getInstance(FormulaErrorValue errorValue) {
    return new EvaluationException(errorValue);
  }

  public FormulaErrorValue getErrorValue() {
    return errorValue;
  }
}
