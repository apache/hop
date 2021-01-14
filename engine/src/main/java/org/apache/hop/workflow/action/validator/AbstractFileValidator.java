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
import org.apache.hop.core.variables.IVariables;

import java.util.List;

public abstract class AbstractFileValidator implements IActionValidator {

  private static final String KEY_VARIABLE_SPACE = "org.apache.hop.workflow.actions.file.variables";

  public static ValidatorContext putVariableSpace( IVariables variables ) {
    ValidatorContext context = new ValidatorContext();
    context.put( KEY_VARIABLE_SPACE, variables );
    return context;
  }

  protected IVariables getVariableSpace( ICheckResultSource source, String propertyName,
                                         List<ICheckResult> remarks, ValidatorContext context ) {
    Object obj = context.get( KEY_VARIABLE_SPACE );
    if ( obj instanceof IVariables ) {
      return (IVariables) obj;
    } else {
      ActionValidatorUtils.addGeneralRemark(
        source, propertyName, getName(), remarks, "messages.failed.missingKey",
        ICheckResult.TYPE_RESULT_ERROR );
      return null;
    }
  }

  public static void putVariableSpace( ValidatorContext context, IVariables variables ) {
    context.put( KEY_VARIABLE_SPACE, variables );
  }

  public AbstractFileValidator() {
    super();
  }

}
