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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Fails if a field's value is <code>null</code>.
 *
 * @author mlowery
 */
public class NotNullValidator implements IActionValidator {

  public static final NotNullValidator INSTANCE = new NotNullValidator();

  private static final String VALIDATOR_NAME = "notNull";

  public boolean validate( ICheckResultSource source, String propertyName,
                           List<ICheckResult> remarks, ValidatorContext context ) {
    Object value = null;
    try {
      value = PropertyUtils.getProperty( source, propertyName );
      if ( null == value ) {
        ActionValidatorUtils.addFailureRemark(
          source, propertyName, VALIDATOR_NAME, remarks, ActionValidatorUtils.getLevelOnFail(
            context, VALIDATOR_NAME ) );
        return false;
      } else {
        return true;
      }
    } catch ( IllegalAccessException e ) {
      ActionValidatorUtils.addExceptionRemark( source, propertyName, VALIDATOR_NAME, remarks, e );
    } catch ( InvocationTargetException e ) {
      ActionValidatorUtils.addExceptionRemark( source, propertyName, VALIDATOR_NAME, remarks, e );
    } catch ( NoSuchMethodException e ) {
      ActionValidatorUtils.addExceptionRemark( source, propertyName, VALIDATOR_NAME, remarks, e );
    }
    return false;
  }

  public String getName() {
    return VALIDATOR_NAME;
  }

}
