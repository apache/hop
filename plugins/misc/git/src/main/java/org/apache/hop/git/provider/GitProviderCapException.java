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

package org.apache.hop.git.provider;

import org.apache.hop.core.exception.HopException;

/**
 * Raised when a provider refuses to paginate further because of a documented limit of its own,
 * rather than because a request failed.
 *
 * <p>This is not an error: the rows already read are complete and correct, there are simply no more
 * available through this endpoint. Loaders catch it, stop paging and record the reason so the
 * transform can report the cap in its summary. Contrast with {@link GitPaginationException}, which
 * signals a genuine failure and aborts the transform.
 */
class GitProviderCapException extends HopException {

  GitProviderCapException(String message) {
    super(message);
  }
}
