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

package org.apache.hop.core.database.dml;

import lombok.Getter;

/**
 * The upsert form chosen for one connection and one {@link DmlSpec}, see {@link DmlSupport#plan},
 * or why there is none.
 */
@Getter
public final class DmlPlan {

  /** The dialect's declaration; null when it declares nothing. */
  private final DmlSyntax syntax;

  /** The chosen upsert form; null when the connection can't write the upsert. */
  private final DmlSyntax.Upsert upsert;

  /** Why the upsert can't be written on this connection; null when it can. */
  private final String unsupportedReason;

  private DmlPlan(DmlSyntax syntax, DmlSyntax.Upsert upsert, String unsupportedReason) {
    this.syntax = syntax;
    this.upsert = upsert;
    this.unsupportedReason = unsupportedReason;
  }

  /** A plan that writes the given upsert form of the syntax. */
  public static DmlPlan of(DmlSyntax syntax, DmlSyntax.Upsert upsert) {
    if (syntax == null || upsert == null) {
      throw new IllegalArgumentException("A supported plan needs a syntax and an upsert form");
    }
    return new DmlPlan(syntax, upsert, null);
  }

  /** A plan that says why the upsert can't be written. */
  public static DmlPlan unsupported(DmlSyntax syntax, String reason) {
    return new DmlPlan(syntax, null, reason);
  }

  public boolean isSupported() {
    return unsupportedReason == null;
  }
}
