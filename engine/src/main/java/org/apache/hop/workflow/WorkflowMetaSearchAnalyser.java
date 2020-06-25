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
import org.apache.hop.workflow.action.ActionCopy;
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

    matchProperty( searchable, results, searchQuery, "workflow name", workflowMeta.getName() );
    matchProperty( searchable, results, searchQuery, "workflow description", workflowMeta.getDescription() );

    // The actions...
    //
    for ( ActionCopy actionCopy : workflowMeta.getActionCopies() ) {
      matchProperty( searchable, results, searchQuery, "workflow action name", actionCopy.getName() );
      matchProperty( searchable, results, searchQuery, "workflow action description", actionCopy.getDescription() );

      IAction action = actionCopy.getAction();
      if (action!=null) {

        String actionPluginId = action.getPluginId();
        if ( actionPluginId != null ) {
          matchProperty( searchable, results, searchQuery, "workflow action plugin ID", actionPluginId );
          IPlugin actionPlugin = PluginRegistry.getInstance().findPluginWithId( ActionPluginType.class, actionPluginId );
          if ( actionPlugin != null ) {
            matchProperty( searchable, results, searchQuery, "workflow action plugin name", actionPlugin.getName() );
          }
        }

        // Search the action properties
        //
        matchObjectFields( searchable, results, searchQuery, action, "workflow action property" );
      }
    }

    // Search the notes...
    //
    for ( NotePadMeta note : workflowMeta.getNotes() ) {
      matchProperty( searchable, results, searchQuery, "workflow note", note.getNote() );
    }

    return results;
  }
}
