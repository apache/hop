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

package org.apache.hop.workflow.action.validator;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;

import java.util.List;

/**
 * Boolean ANDs the results of all validators. If one validator fails, <code>false</code> is immediately returned. The
 * validators list (a <code>List&lt;IActionValidator></code>) should be stored under the <code>KEY_VALIDATORS</code>
 * key.
 *
 * @author mlowery
 */
public class AndValidator implements IActionValidator {

  public static final AndValidator INSTANCE = new AndValidator();

  private static final String KEY_VALIDATORS = "validators";

  private static final String VALIDATOR_NAME = "and";

  public boolean validate( ICheckResultSource source, String propertyName,
                           List<ICheckResult> remarks, ValidatorContext context ) {
    // Object o = context.get(KEY_VALIDATORS);

    Object[] validators = (Object[]) context.get( KEY_VALIDATORS );
    for ( Object validator : validators ) {
      if ( !( (IActionValidator) validator ).validate( source, propertyName, remarks, context ) ) {
        // failure remarks have already been saved
        return false;
      }
    }
    ActionValidatorUtils.addOkRemark( source, propertyName, remarks );
    return true;
  }

  public String getName() {
    return VALIDATOR_NAME;
  }

  public String getKeyValidators() {
    return KEY_VALIDATORS;
  }

  /**
   * Uses varargs to conveniently add validators to the list of validators consumed by <code>AndValidator</code>. This
   * method creates and returns a new context.
   *
   * @see #putValidators(ValidatorContext, IActionValidator[])
   */
  public static ValidatorContext putValidators( IActionValidator... validators ) {
    ValidatorContext context = new ValidatorContext();
    context.put( AndValidator.KEY_VALIDATORS, validators );
    return context;
  }

  /**
   * Uses varargs to conveniently add validators to the list of validators consumed by <code>AndValidator</code>. This
   * method adds to an existing map.
   *
   * @see #putValidators(IActionValidator[])
   */
  public static void putValidators( ValidatorContext context, IActionValidator... validators ) {
    context.put( AndValidator.KEY_VALIDATORS, validators );
  }

}
