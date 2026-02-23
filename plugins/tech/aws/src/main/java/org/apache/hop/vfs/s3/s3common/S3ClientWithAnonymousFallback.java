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

package org.apache.hop.vfs.s3.s3common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Wraps an S3 client so that when a call fails with 403 Access Denied, the same call is retried
 * using an anonymous (no-credentials) client. This allows using the default credential chain (env,
 * IAM, profile) while still succeeding for public buckets when those credentials lack access.
 */
public final class S3ClientWithAnonymousFallback implements InvocationHandler {

  private final S3Client primary;
  private final S3Client anonymous;

  private S3ClientWithAnonymousFallback(S3Client primary, S3Client anonymous) {
    this.primary = primary;
    this.anonymous = anonymous;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(primary, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (isAccessDenied(cause)) {
        try {
          return method.invoke(anonymous, args);
        } catch (InvocationTargetException e2) {
          throw e2.getCause() != null ? e2.getCause() : e2;
        }
      }
      throw cause;
    }
  }

  private static boolean isAccessDenied(Throwable t) {
    if (!(t instanceof S3Exception)) {
      return false;
    }
    Integer code = ((S3Exception) t).statusCode();
    return code != null && code == 403;
  }

  /**
   * Returns an S3Client that delegates to {@code primary} and on 403 retries the same call with
   * {@code anonymous}. Both clients must use the same region/endpoint configuration.
   */
  public static S3Client create(S3Client primary, S3Client anonymous) {
    return (S3Client)
        Proxy.newProxyInstance(
            S3Client.class.getClassLoader(),
            new Class<?>[] {S3Client.class},
            new S3ClientWithAnonymousFallback(primary, anonymous));
  }
}
