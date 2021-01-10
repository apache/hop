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
 * The interface of a action validator.
 *
 * <p>
 * Action validators can provide convenience methods for adding information to the validator context. Those methods
 * should following a naming convention: putX where X is the name of the object being adding to the context. An example:
 * <ul>
 * <li>ValidatorContext putSomeObject(Object someObject)</li>
 * <li>void putSomeObject(ValidatorContext context, Object someObject)</li>
 * </ul>
 * </p>
 *
 * @author mlowery
 */
public interface IActionValidator {

  String KEY_LEVEL_ON_FAIL = "levelOnFail";

  /**
   * Using reflection, the validator fetches the field named <code>propertyName</code> from the bean <code>source</code>
   * and runs the validation putting any messages into <code>remarks</code>. The return value is <code>true</code> if
   * the validation passes.
   *
   * @param source       bean to validate
   * @param propertyName property to validate
   * @param remarks      list to which to add messages
   * @param context      any other information needed to perform the validation
   * @return validation result
   */
  boolean validate( ICheckResultSource source, String propertyName, List<ICheckResult> remarks,
                    ValidatorContext context );

  /**
   * Returns the name of this validator, unique among all validators.
   *
   * @return name
   */
  String getName();
}
