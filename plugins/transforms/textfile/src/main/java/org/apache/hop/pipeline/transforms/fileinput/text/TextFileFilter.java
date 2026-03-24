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

package org.apache.hop.pipeline.transforms.fileinput.text;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.Base64StringEncoder;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class TextFileFilter implements Cloneable {
  /** The position of the occurrence of the filter string to check at */
  @HopMetadataProperty(
      key = "filter_position",
      injectionKey = "FILTER_POSITION",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_POSITION")
  private int filterPosition;

  /** The string to filter on */
  @HopMetadataProperty(
      key = "filter_string",
      stringEncoder = Base64StringEncoder.class,
      injectionKey = "FILTER_STRING",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_STRING")
  private String filterString;

  /** True if we want to stop when we reach a filter line */
  @HopMetadataProperty(
      key = "filter_is_last_line",
      injectionKey = "FILTER_LAST_LINE",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_LAST_LINE")
  private boolean filterLastLine;

  /** True if we want to match only this lines */
  @HopMetadataProperty(
      key = "filter_is_positive",
      injectionKey = "FILTER_POSITIVE",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_POSITIVE")
  private boolean filterPositive;

  /**
   * @param filterPosition The position of the occurrence of the filter string to check at
   * @param filterString The string to filter on
   * @param filterLastLine True if we want to stop when we reach a filter string on the specified
   *     position False if you just want to skip the line.
   * @param filterPositive True if we want to get only lines that match this string
   */
  public TextFileFilter(
      int filterPosition, String filterString, boolean filterLastLine, boolean filterPositive) {
    this.filterPosition = filterPosition;
    this.filterString = filterString;
    this.filterLastLine = filterLastLine;
    this.filterPositive = filterPositive;
  }

  public TextFileFilter() {}

  public TextFileFilter(TextFileFilter f) {
    this.filterPosition = f.filterPosition;
    this.filterString = f.filterString;
    this.filterLastLine = f.filterLastLine;
    this.filterPositive = f.filterPositive;
  }

  @Override
  public Object clone() {
    return new TextFileFilter(this);
  }
}
