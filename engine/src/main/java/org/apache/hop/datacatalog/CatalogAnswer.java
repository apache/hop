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

package org.apache.hop.datacatalog;

/**
 * What a data catalog said. A catalog that can't be reached, refuses the question or doesn't know
 * the answer is a normal outcome, not an exception: the caller decides what that means for its run.
 * Experimental.
 *
 * @param <T> the type of the answer
 */
public sealed interface CatalogAnswer<T>
    permits CatalogAnswer.Known, CatalogAnswer.NotFound, CatalogAnswer.Unknown {

  /** Why a catalog could not answer. */
  enum Reason {
    /** The catalog type can't answer this kind of question. */
    NOT_SUPPORTED,
    /** Hop could not tell the catalog which dataset it means, for example a file without a path. */
    NOT_RESOLVABLE,
    /** The catalog could not be reached. */
    UNREACHABLE,
    /** The catalog refused the credentials, or they may not read this. */
    UNAUTHORIZED,
    /** The catalog did not answer in time. */
    TIMEOUT,
    /** The catalog answered with an error. */
    ERROR
  }

  /** The catalog knows the answer. */
  record Known<T>(T value) implements CatalogAnswer<T> {}

  /** The catalog has no such dataset, or nothing recorded for the question. */
  record NotFound<T>() implements CatalogAnswer<T> {}

  /** The catalog could not answer. */
  record Unknown<T>(Reason reason, String detail) implements CatalogAnswer<T> {}

  static <T> CatalogAnswer<T> known(T value) {
    return new Known<>(value);
  }

  static <T> CatalogAnswer<T> notFound() {
    return new NotFound<>();
  }

  static <T> CatalogAnswer<T> unknown(Reason reason, String detail) {
    return new Unknown<>(reason, detail);
  }

  /** The same failure to answer, for a question of another type. */
  default <R> CatalogAnswer<R> unknownAs() {
    if (this instanceof Unknown<T> unknown) {
      return new Unknown<>(unknown.reason(), unknown.detail());
    }
    throw new IllegalStateException("Not an unknown answer: " + this);
  }
}
