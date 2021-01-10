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

package org.apache.hop.workflow;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.BaseSearchableAnalyser;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchableAnalyserPlugin;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "WorkflowMetaSearchAnalyser",
  name = "Search in workflow metadata"
)
public class WorkflowMetaSearchAnalyser extends BaseSearchableAnalyser<WorkflowMeta> implements ISearchableAnalyser<WorkflowMeta> {

  @Override public Class<WorkflowMeta> getSearchableClass() {
    return WorkflowMeta.class;
  }

  @Override public List<ISearchResult> search( ISearchable<WorkflowMeta> searchable, ISearchQuery searchQuery  ) {
    WorkflowMeta workflowMeta = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "workflow name", workflowMeta.getName(), null);
    matchProperty( searchable, results, searchQuery, "workflow description", workflowMeta.getDescription(), null );

    // The actions...
    //
    for ( ActionMeta actionMeta : workflowMeta.getActions() ) {
      String actionName = actionMeta.getName();
      matchProperty( searchable, results, searchQuery, "workflow action name", actionName, actionName );
      matchProperty( searchable, results, searchQuery, "workflow action description", actionMeta.getDescription(), actionName );

      IAction action = actionMeta.getAction();
      if (action!=null) {

        String actionPluginId = action.getPluginId();
        if ( actionPluginId != null ) {
          matchProperty( searchable, results, searchQuery, "workflow action plugin ID", actionPluginId, actionName );
          IPlugin actionPlugin = PluginRegistry.getInstance().findPluginWithId( ActionPluginType.class, actionPluginId );
          if ( actionPlugin != null ) {
            matchProperty( searchable, results, searchQuery, "workflow action plugin name", actionPlugin.getName(), actionName );
          }
        }

        // Search the action properties
        //
        matchObjectFields( searchable, results, searchQuery, action, "workflow action property", actionName );
      }
    }

    // Search the notes...
    //
    for ( NotePadMeta note : workflowMeta.getNotes() ) {
      matchProperty( searchable, results, searchQuery, "workflow note", note.getNote(), null );
    }

    return results;
  }
}
