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

package org.apache.hop.pipeline.transforms.httppost;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;

public class HttpPostLookupField {

  @HopMetadataProperty(
      key = "query",
      injectionGroupDescription = "HTTPPOST.Injection.LookupQueryField")
  private List<HttpPostQuery> queryField = new ArrayList<>();

  @HopMetadataProperty(
      key = "arg",
      injectionGroupDescription = "HTTPPOST.Injection.LookupArgumentField")
  private List<HttpPostArgumentField> argumentField = new ArrayList<>();

  public List<HttpPostQuery> getQueryField() {
    return queryField;
  }

  public void setQueryField(List<HttpPostQuery> queryField) {
    this.queryField = queryField;
  }

  public List<HttpPostArgumentField> getArgumentField() {
    return argumentField;
  }

  public void setArgumentField(List<HttpPostArgumentField> argumentField) {
    this.argumentField = argumentField;
  }

  public HttpPostLookupField(
      List<HttpPostQuery> postQuery, List<HttpPostArgumentField> argumentField) {
    this.queryField = postQuery;
    this.argumentField = argumentField;
  }

  public HttpPostLookupField(HttpPostLookupField httpPostLookupField) {
    this.queryField = httpPostLookupField.queryField;
    this.argumentField = httpPostLookupField.argumentField;
  }

  public HttpPostLookupField() {}
}
