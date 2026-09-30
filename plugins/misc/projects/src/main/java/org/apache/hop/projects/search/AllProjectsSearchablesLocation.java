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

package org.apache.hop.projects.search;

import java.util.Iterator;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Search location over every configured project and the configuration files of its environments.
 */
public class AllProjectsSearchablesLocation implements ISearchablesLocation {

  public static final String LOCATION_ID = "all-projects";

  public static final String DESCRIPTION = "All projects";

  @Override
  public String getLocationDescription() {
    return DESCRIPTION;
  }

  @Override
  public String getLocationId() {
    return LOCATION_ID;
  }

  @Override
  public boolean isIncludedInDefaultSearch() {
    return false;
  }

  @Override
  public Iterator<ISearchable> getSearchables(
      IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    // metadataProvider belongs to the active project. Each configured project is loaded on its own.
    return new AllProjectsSearchablesIterator(variables);
  }
}
