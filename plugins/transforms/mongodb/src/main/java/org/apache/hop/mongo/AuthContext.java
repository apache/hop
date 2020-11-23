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

package org.apache.hop.mongo;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * A context for executing authorized actions on behalf of an authenticated user via a {@link
 * LoginContext}.
 */
public class AuthContext {
  private LoginContext login;

  /**
   * Create a context for the given login. If the login is null all operations will be done as the
   * current user.
   *
   * <p>TODO Prevent null login contexts and create login contexts for the current OS user instead.
   * This will keep the implementation cleaner.
   *
   * @param login
   */
  public AuthContext(LoginContext login) {
    this.login = login;
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no user is explicitly
   * authenticated the action will be executed as the current user.
   *
   * @param action The action to execute
   * @return The return value of the action
   */
  public <T> T doAs(PrivilegedAction<T> action) {
    if (login == null) {
      // If a user is not explicitly authenticated directly execute the action
      return action.run();
    } else {
      return Subject.doAs(login.getSubject(), action);
    }
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no user is explicitly
   * authenticated the action will be executed as the current user.
   *
   * @param action The action to execute
   * @return The return value of the action
   * @throws PrivilegedActionException If an exception occurs while executing the action. The cause
   *     of the exception will be provided in {@link PrivilegedActionException#getCause()}.
   */
  public <T> T doAs(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
    if (login == null) {
      // If a user is not explicitly authenticated directly execute the action
      try {
        return action.run();
      } catch (Exception ex) {
        // Wrap any exceptions throw in a PrivilegedActionException just as
        // would be thrown when executed via Subject.doAs(..)
        throw new PrivilegedActionException(ex);
      }
    } else {
      return Subject.doAs(login.getSubject(), action);
    }
  }
}
