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
 * Thrown when a provider request fails partway through pagination.
 *
 * <p>The transform stops rather than emitting what it managed to collect: an empty page is
 * indistinguishable from the end of the data, so continuing would report a successful run with a
 * silently incomplete result set. The request has already been retried by {@link GitApiHttp} before
 * this is raised, so the failure is not a transient blip.
 */
public class GitPaginationException extends HopException {

  private final int page;

  GitPaginationException(String providerLabel, int page, Throwable cause) {
    super(
        providerLabel
            + " pagination failed at page "
            + page
            + ", after the request had already been retried. Stopping instead of returning a"
            + " partial result set. Re-run the pipeline, or lower the page size if the provider is"
            + " rejecting large pages.",
        cause);
    this.page = page;
  }

  public int getPage() {
    return page;
  }
}
