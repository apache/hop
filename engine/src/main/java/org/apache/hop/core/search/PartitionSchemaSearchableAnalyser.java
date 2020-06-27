package org.apache.hop.core.search;

import org.apache.hop.partition.PartitionSchema;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "PartitionSchemaSearchableAnalyser",
  name = "Search in partition schema metadata"
)
public class PartitionSchemaSearchableAnalyser extends BaseSearchableAnalyser<PartitionSchema> implements ISearchableAnalyser<PartitionSchema> {

  @Override public Class<PartitionSchema> getSearchableClass() {
    return PartitionSchema.class;
  }

  @Override public List<ISearchResult> search( ISearchable<PartitionSchema> searchable, ISearchQuery searchQuery ) {
    PartitionSchema partitionSchema = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "Partition schema name", partitionSchema.getName(), null );
    matchProperty( searchable, results, searchQuery, "Partition schema number of partitions", partitionSchema.getNumberOfPartitions(), null );
    return results;
  }
}
