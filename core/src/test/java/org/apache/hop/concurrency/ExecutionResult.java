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

package org.apache.hop.concurrency;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Andrey Khayrutdinov
 */
class ExecutionResult<T> {
  static <T> ExecutionResult<T> from( Future<? extends T> future ) {
    try {
      return new ExecutionResult<>( future.get(), null );
    } catch ( InterruptedException e ) {
      throw new IllegalArgumentException( e );
    } catch ( ExecutionException e ) {
      return new ExecutionResult<>( null, e.getCause() );
    }
  }

  private final T result;
  private final Throwable throwable;

  ExecutionResult( T result, Throwable throwable ) {
    this.result = result;
    this.throwable = throwable;
  }

  boolean isError() {
    return ( throwable != null );
  }

  T getResult() {
    return result;
  }

  Throwable getThrowable() {
    return throwable;
  }
}
