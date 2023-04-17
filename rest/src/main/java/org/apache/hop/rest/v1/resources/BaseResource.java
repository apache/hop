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
 *
 */

package org.apache.hop.rest.v1.resources;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.hop.core.Const;
import org.apache.hop.rest.Hop;

public abstract class BaseResource {
  protected final Hop hop = Hop.getInstance();

  protected Response getServerError(String errorMessage) {
    return getServerError(errorMessage, null, true);
  }

  protected Response getServerError(String errorMessage, boolean logOnServer) {
    return getServerError(errorMessage, null, logOnServer);
  }

  protected Response getServerError(String errorMessage, Exception e) {
    return getServerError(errorMessage, e, true);
  }

  protected Response getServerError(String errorMessage, Exception e, boolean logOnServer) {
    if (logOnServer) {
      if (e != null) {
        hop.getLog().logError(errorMessage, e);
      } else {
        hop.getLog().logError(errorMessage);
      }
    }
    return Response.serverError()
        .status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(errorMessage + (e == null ? "" : ("\n" + Const.getSimpleStackTrace(e))))
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
