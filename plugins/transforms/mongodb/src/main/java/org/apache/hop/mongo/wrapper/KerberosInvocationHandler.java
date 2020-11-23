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

package org.apache.hop.mongo.wrapper;

import org.apache.hop.mongo.AuthContext;
import org.apache.hop.mongo.MongoDbException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Handles proxying all method calls through an AuthContext. This allows methods to be executed as
 * an authenticated user via a LoginContext.
 */
public class KerberosInvocationHandler implements InvocationHandler {
  private final AuthContext authContext;
  private final Object delegate;

  public KerberosInvocationHandler(AuthContext authContext, Object delegate) {
    this.authContext = authContext;
    this.delegate = delegate;
  }

  @Override
  public Object invoke(Object proxy, final Method method, final Object[] args)
      throws MongoDbException {
    try {
      return authContext.doAs(
          new PrivilegedExceptionAction<Object>() {

            @Override
            public Object run() throws Exception {
              try {
                return method.invoke(delegate, args);
              } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                  throw (Exception) cause;
                }
                throw e;
              }
            }
          });
    } catch (PrivilegedActionException e) {
      if (e.getCause() instanceof MongoDbException) {
        throw (MongoDbException) e.getCause();
      } else {
        throw new MongoDbException(e.getCause());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T wrap(Class<T> iface, AuthContext authContext, Object delegate) {
    return (T)
        Proxy.newProxyInstance(
            KerberosInvocationHandler.class.getClassLoader(),
            new Class<?>[] {iface},
            new KerberosInvocationHandler(authContext, delegate));
  }
}
