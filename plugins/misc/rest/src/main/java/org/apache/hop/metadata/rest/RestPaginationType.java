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

package org.apache.hop.metadata.rest;

/**
 * Describes how pagination tokens are propagated for REST requests configured on {@link
 * RestConnection}.
 */
public enum RestPaginationType {
  /** No pagination (default). */
  NONE,

  /**
   * Use the RFC 5988 {@code Link} response header: follow the bracketed URI whose {@code rel}
   * relation type includes {@code next}. Multiple header fields and comma-separated link-values are
   * supported ({@code prev} and others are ignored unless they advertise {@code next}).
   */
  LINK_HEADER,

  /** Encode {@code offset} and {@code limit} query parameters (names from metadata). */
  OFFSET_LIMIT,

  /**
   * Encode a {@code page} (or similarly named) query parameter with an incrementing page index,
   * starting at 1.
   */
  PAGE_NUMBER,

  /**
   * Encode a cursor token query parameter ({@link RestConnection#pageParamName}, typically named
   * {@code cursor}) from the previous response body using JsonPath/XPath expressions in metadata.
   */
  CURSOR,

  /**
   * Merge cursor and batch-size fields into the HTTP request body on POST/PUT/PATCH (form
   * url-encoded or JSON). The next cursor is read from the response with JsonPath/XPath (as with
   * {@link #CURSOR}). Pagination stops when the next cursor is empty — not when a page returns zero
   * rows (Slack-style APIs may return sparse pages while {@code next_cursor} is still set).
   *
   * <p>On GET requests, cursor and batch-size are sent as query parameters instead (HTTP clients
   * typically ignore GET bodies).
   */
  BODY_CURSOR,

  /**
   * Send the cursor token as an HTTP request header ({@link RestConnection#pageParamName}). The
   * next cursor is read from the response with JsonPath/XPath. Pagination stops when the next
   * cursor is empty.
   */
  HEADER_CURSOR,

  /**
   * Read the next page URL from the JSON/XML response body (JsonPath/XPath on the connection) and
   * request it as an absolute URL on the following iteration (same loop semantics as {@link
   * #LINK_HEADER}).
   */
  BODY_NEXT_URL
}
