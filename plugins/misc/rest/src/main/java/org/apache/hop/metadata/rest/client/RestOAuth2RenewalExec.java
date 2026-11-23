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

package org.apache.hop.metadata.rest.client;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hc.client5.http.classic.ExecChain;
import org.apache.hc.client5.http.classic.ExecChainHandler;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;

/**
 * Sends a request once more with a new OAuth 2 access token when the server answers 401 to a token
 * the connection fetched (issue #9550). It sits in the client's execution chain, so every caller of
 * a REST connection gets it: the REST client, HTTP client and HTTP POST transforms, the HTTP action
 * and the connection test.
 *
 * <p>It is the outermost element of the chain, so it sees the final answer after redirects and
 * challenge-response authentication, and the request it resends is the caller's own. A request
 * whose body cannot be read a second time (a streamed upload) is not resent; its token is still
 * dropped, so the next request fetches a new one.
 */
final class RestOAuth2RenewalExec implements ExecChainHandler {

  static final String NAME = "hop-oauth2-token-renewal";

  private static final Class<?> PKG = RestAuthenticator.class;

  private final RestAuthenticator authenticator;
  private final ILogChannel log;

  RestOAuth2RenewalExec(RestAuthenticator authenticator, ILogChannel log) {
    this.authenticator = authenticator;
    this.log = log;
  }

  @Override
  public ClassicHttpResponse execute(
      ClassicHttpRequest request, ExecChain.Scope scope, ExecChain chain)
      throws IOException, HttpException {
    ClassicHttpResponse response = chain.proceed(request, scope);
    if (response.getCode() != HttpStatus.SC_UNAUTHORIZED) {
      return response;
    }

    ClassicHttpRequest original = scope.originalRequest;
    Header authorization = original.getFirstHeader(HttpHeaders.AUTHORIZATION);
    if (authorization == null
        || !authenticator.isRenewable(authorization.getValue(), requestUri(original))) {
      return response;
    }

    HttpEntity entity = original.getEntity();
    if (entity != null && !entity.isRepeatable()) {
      authenticator.dropRejectedToken(authorization.getValue());
      return response;
    }

    String renewed;
    try {
      renewed = authenticator.renewRejectedToken(authorization.getValue());
    } catch (HopException e) {
      // The request keeps its 401, as it would have without a renewal.
      if (log != null) {
        log.logError(
            BaseMessages.getString(
                PKG, "RestAuthenticator.Log.OAuth2TokenRenewalFailed", e.getMessage()));
      }
      return response;
    }
    if (renewed == null) {
      return response;
    }

    EntityUtils.consume(response.getEntity());
    response.close();
    if (log != null) {
      log.logBasic(
          BaseMessages.getString(PKG, "RestAuthenticator.Log.OAuth2TokenRejectedRetrying"));
    }

    ClassicHttpRequest resend = ClassicRequestBuilder.copy(original).build();
    resend.setHeader(HttpHeaders.AUTHORIZATION, renewed);
    ClassicHttpResponse second = chain.proceed(resend, scope);
    if (second.getCode() == HttpStatus.SC_UNAUTHORIZED) {
      authenticator.renewedTokenRejected(renewed);
    }
    return second;
  }

  private static String requestUri(ClassicHttpRequest request) {
    try {
      return request.getUri().toString();
    } catch (URISyntaxException e) {
      return null;
    }
  }
}
