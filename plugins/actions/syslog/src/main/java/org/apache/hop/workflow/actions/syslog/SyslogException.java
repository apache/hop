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

package org.apache.hop.workflow.actions.syslog;

import org.apache.hop.core.exception.HopException;

/**
 * This exception is throws when and error is found in a Syslog sending process.
 *
 * @author Samatar
 * @since 01-01-2010
 */

public class SyslogException extends HopException {

  public static final long serialVersionUID = -1;

  /**
   * Constructs a new throwable with null as its detail message.
   */
  public SyslogException() {
    super();
  }

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message - the detail message. The detail message is saved for later retrieval by the getMessage() method.
   */
  public SyslogException( String message ) {
    super( message );
  }

}
