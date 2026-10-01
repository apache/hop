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

package org.apache.hop.pipeline.transforms.rest;

/**
 * How to split a streaming response into records (issue #2746). Both formats have an unambiguous
 * record boundary, which is what makes it possible to emit a row before the response has ended.
 */
public enum RestStreamingFormat {
  /**
   * Newline-delimited JSON: one record per line. What bulk-export APIs generally serve, and what
   * {@code application/x-ndjson} and {@code application/jsonl} mean.
   */
  NDJSON,

  /**
   * Server-Sent Events (the WHATWG {@code text/event-stream} format): records are separated by a
   * blank line, and the value of a record is its {@code data:} field. Other fields ({@code event},
   * {@code id}, {@code retry}) and comment lines are skipped, so what reaches the row is the
   * payload rather than the framing.
   */
  SSE
}
