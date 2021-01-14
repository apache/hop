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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.Iterator;

public class HopGuiSearchLocation implements ISearchablesLocation {
  protected HopGui hopGui;

  public HopGuiSearchLocation( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public String getLocationDescription() {
    return "Current objects loaded in the Hop GUI";
  }

  @Override public Iterator<ISearchable> getSearchables() throws HopException {
    return new HopGuiSearchLocationIterator( hopGui, this );
  }
}
