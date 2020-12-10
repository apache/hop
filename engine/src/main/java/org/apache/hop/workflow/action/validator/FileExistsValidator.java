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

import org.apache.commons.validator.util.ValidatorUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

import java.io.IOException;
import java.util.List;

/**
 * Fails if a field's value is a filename and the file does not exist.
 *
 * @author mlowery
 */
public class FileExistsValidator extends AbstractFileValidator {

  public static final FileExistsValidator INSTANCE = new FileExistsValidator();

  static final String VALIDATOR_NAME = "fileExists";

  private static final String KEY_FAIL_IF_DOES_NOT_EXIST =
    "org.apache.hop.workflow.actions.createfile.failIfDoesNotExist";

  public boolean validate( ICheckResultSource source, String propertyName,
                           List<ICheckResult> remarks, ValidatorContext context ) {

    String filename = ValidatorUtils.getValueAsString( source, propertyName );
    IVariables variables = getVariableSpace( source, propertyName, remarks, context );
    boolean failIfDoesNotExist = getFailIfDoesNotExist( source, propertyName, remarks, context );

    if ( null == variables ) {
      return false;
    }

    String realFileName = variables.resolve( filename );
    FileObject fileObject = null;
    try {
      fileObject = HopVfs.getFileObject( realFileName );
      if ( fileObject == null || ( fileObject != null && !fileObject.exists() && failIfDoesNotExist ) ) {
        ActionValidatorUtils.addFailureRemark(
          source, propertyName, VALIDATOR_NAME, remarks, ActionValidatorUtils.getLevelOnFail(
            context, VALIDATOR_NAME ) );
        return false;
      }
      try {
        fileObject.close(); // Just being paranoid
      } catch ( IOException ignored ) {
        // Ignore close errors
      }
    } catch ( Exception e ) {
      ActionValidatorUtils.addExceptionRemark( source, propertyName, VALIDATOR_NAME, remarks, e );
      return false;
    }
    return true;
  }

  public String getName() {
    return VALIDATOR_NAME;
  }

  public static ValidatorContext putFailIfDoesNotExist( boolean failIfDoesNotExist ) {
    ValidatorContext context = new ValidatorContext();
    context.put( KEY_FAIL_IF_DOES_NOT_EXIST, failIfDoesNotExist );
    return context;
  }

  protected boolean getFailIfDoesNotExist( ICheckResultSource source, String propertyName,
                                           List<ICheckResult> remarks, ValidatorContext context ) {
    Object obj = context.get( KEY_FAIL_IF_DOES_NOT_EXIST );
    if ( obj instanceof Boolean ) {
      return (Boolean) obj;
    } else {
      // default is false
      return false;
    }
  }

  public static void putFailIfDoesNotExist( ValidatorContext context, boolean failIfDoesNotExist ) {
    context.put( KEY_FAIL_IF_DOES_NOT_EXIST, failIfDoesNotExist );
  }

}
