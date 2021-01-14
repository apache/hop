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

package org.apache.hop.core.exception;

import org.apache.hop.core.Const;

/**
 * This is an exception thrown by file handling (I/O) when an End Of File marker has been reached.
 *
 * @author Matt
 * @since 30-MAR-2004
 */
public class HopFileException extends HopException {
  public static final long serialVersionUID = 0x8D8EA0264F7A1C13L;

  /**
   * Constructs a new throwable with null as its detail message.
   */
  public HopFileException() {
    super();
  }

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message - the detail message. The detail message is saved for later retrieval by the getMessage() method.
   */
  public HopFileException( String message ) {
    super( message );
  }

  /**
   * Constructs a new throwable with the specified cause and a detail message of (cause==null ? null : cause.toString())
   * (which typically contains the class and detail message of cause).
   *
   * @param cause the cause (which is saved for later retrieval by the getCause() method). (A null value is permitted, and
   *              indicates that the cause is nonexistent or unknown.)
   */
  public HopFileException( Throwable cause ) {
    super( cause );
  }

  /**
   * Constructs a new throwable with the specified detail message and cause.
   *
   * @param message the detail message (which is saved for later retrieval by the getMessage() method).
   * @param cause   the cause (which is saved for later retrieval by the getCause() method). (A null value is permitted, and
   *                indicates that the cause is nonexistent or unknown.)
   */
  public HopFileException( String message, Throwable cause ) {
    super( message, cause );
  }

  /**
   * get the messages back to it's origin cause.
   */
  @Override
  public String getMessage() {
    String retval = Const.CR;
    retval += super.getMessage() + Const.CR;

    Throwable cause = getCause();
    if ( cause != null ) {
      String message = cause.getMessage();
      if ( message != null ) {
        retval += message + Const.CR;
      } else {
        // Add with stack trace elements of cause...
        StackTraceElement[] ste = cause.getStackTrace();
        for ( int i = ste.length - 1; i >= 0; i-- ) {
          retval +=
            " at "
              + ste[ i ].getClassName() + "." + ste[ i ].getMethodName() + " (" + ste[ i ].getFileName() + ":"
              + ste[ i ].getLineNumber() + ")" + Const.CR;
        }
      }
    }

    return retval;
  }

}
