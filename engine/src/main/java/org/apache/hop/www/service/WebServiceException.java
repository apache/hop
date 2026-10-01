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

package org.apache.hop.www.service;

import org.apache.hop.core.exception.HopException;

/**
 * Thrown while resolving a web service request, carrying enough information for a transport to pick
 * a sensible HTTP status without having to parse a message.
 */
public class WebServiceException extends HopException {

  /** Why the request could not be served. */
  public enum Reason {
    /** The request itself is wrong, e.g. no service name was given. */
    BAD_REQUEST,
    /** No web service metadata element with the requested name exists. */
    NOT_FOUND,
    /** The web service exists but is disabled. */
    DISABLED
  }

  private final Reason reason;

  public WebServiceException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public Reason getReason() {
    return reason;
  }
}
