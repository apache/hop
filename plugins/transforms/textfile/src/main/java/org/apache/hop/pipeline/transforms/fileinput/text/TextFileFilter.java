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

import org.apache.commons.codec.binary.Base64;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IStringObjectConverter;

public class TextFileFilter implements Cloneable {

  private static final String STRING_BASE64_PREFIX = "Base64: ";

  /** The position of the occurrence of the filter string to check at */
  @HopMetadataProperty(
      key = "filter_position",
      injectionKey = "FILTER_POSITION",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_POSITION")
  private int filterPosition;

  /** The string to filter on */
  @HopMetadataProperty(
      key = "filter_string",
      injectionKey = "FILTER_STRING",
      injectionKeyDescription = "TextFileInput.Injection.FILTER_STRING",
      injectionStringObjectConverter = FilterStringConverter.class)
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

  @Override
  public Object clone() {
    try {
      Object retval = super.clone();
      return retval;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  /** @return Returns the filterLastLine. */
  public boolean isFilterLastLine() {
    return filterLastLine;
  }

  /** @param filterLastLine The filterLastLine to set. */
  public void setFilterLastLine(boolean filterLastLine) {
    this.filterLastLine = filterLastLine;
  }

  /** @return Returns the filterPositive. */
  public boolean isFilterPositive() {
    return filterPositive;
  }

  /** @param filterPositive The filterPositive to set. */
  public void setFilterPositive(boolean filterPositive) {
    this.filterPositive = filterPositive;
  }

  /** @return Returns the filterPosition. */
  public int getFilterPosition() {
    return filterPosition;
  }

  /** @param filterPosition The filterPosition to set. */
  public void setFilterPosition(int filterPosition) {
    this.filterPosition = filterPosition;
  }

  /** @return Returns the filterString. */
  public String getFilterString() {
    return filterString;
  }

  /** @param filterString The filterString to set. */
  public void setFilterString(String filterString) {
    this.filterString = filterString;
  }

  public static final class FilterStringConverter implements IStringObjectConverter {
    @Override
    public String getString(Object object) throws HopException {
      if (!(object instanceof String)) {
        throw new HopException("We only support XML serialization of String objects here");
      }
      try {
        return STRING_BASE64_PREFIX + new String(Base64.encodeBase64(((String) object).getBytes()));
      } catch (Exception e) {
        throw new HopException("Error serializing filterString to XML", e);
      }
    }

    @Override
    public Object getObject(String s) throws HopException {
      try {
        if (s != null && s.startsWith(STRING_BASE64_PREFIX)) {
          return new String(
              Base64.decodeBase64(s.substring(STRING_BASE64_PREFIX.length()).getBytes()));
        } else {
          return s;
        }
      } catch (Exception e) {
        throw new HopException("Error serializing filterString from XML", e);
      }
    }
  }
}
