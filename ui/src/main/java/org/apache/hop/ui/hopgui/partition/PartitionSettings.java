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

package org.apache.hop.ui.hopgui.partition;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.core.metadata.MetadataManager;

import java.util.Collections;
import java.util.List;

/**
 * @author Evgeniy_Lyakhov@epam.com
 */
public class PartitionSettings {

  private final TransformMeta transformMeta;
  private final PipelineMeta pipelineMeta;
  private final MetadataManager<PartitionSchema> schemaManager;
  private final String[] options;
  private final String[] codes;
  private final TransformMeta before;

  public PartitionSettings( int exactSize, PipelineMeta pipelineMeta, TransformMeta transformMeta, MetadataManager<PartitionSchema> schemaManager ) {
    this.pipelineMeta = pipelineMeta;
    this.transformMeta = transformMeta;
    this.schemaManager = schemaManager;
    this.options = new String[ exactSize ];
    this.codes = new String[ exactSize ];
    this.before = (TransformMeta) transformMeta.clone();
    System.arraycopy( TransformPartitioningMeta.methodDescriptions, 0, options, 0, TransformPartitioningMeta.methodDescriptions.length );
    System.arraycopy( TransformPartitioningMeta.methodCodes, 0, codes, 0, TransformPartitioningMeta.methodCodes.length );
  }

  public void fillOptionsAndCodesByPlugins( List<IPlugin> plugins ) {
    int pluginIndex = 0;
    for ( IPlugin plugin : plugins ) {
      options[ TransformPartitioningMeta.methodDescriptions.length + pluginIndex ] = plugin.getDescription();
      codes[ TransformPartitioningMeta.methodCodes.length + pluginIndex ] = plugin.getIds()[ 0 ];
      pluginIndex++;
    }
  }

  public int getDefaultSelectedMethodIndex() {
    for ( int i = 0; i < codes.length; i++ ) {
      if ( codes[ i ].equals( transformMeta.getTransformPartitioningMeta().getMethod() ) ) {
        return i;
      }
    }
    return 0;
  }

  public int getDefaultSelectedSchemaIndex() {
    List<String> schemaNames;
    try {
      schemaNames = schemaManager.getNames();
    } catch ( HopException e ) {
      schemaNames = Collections.emptyList();
    }

    PartitionSchema partitioningSchema = transformMeta.getTransformPartitioningMeta().getPartitionSchema();
    int defaultSelectedSchemaIndex = 0;
    if ( partitioningSchema != null && partitioningSchema.getName() != null
      && !schemaNames.isEmpty() ) {
      defaultSelectedSchemaIndex =
        Const.indexOfString( partitioningSchema.getName(), schemaNames );
    }
    return defaultSelectedSchemaIndex != -1 ? defaultSelectedSchemaIndex : 0;
  }

  public String getMethodByMethodDescription( String methodDescription ) {
    String method = TransformPartitioningMeta.methodCodes[ TransformPartitioningMeta.PARTITIONING_METHOD_NONE ];
    for ( int i = 0; i < options.length; i++ ) {
      if ( options[ i ].equals( methodDescription ) ) {
        method = codes[ i ];
      }
    }
    return method;
  }

  public String[] getOptions() {
    return options;
  }

  public String[] getCodes() {
    return codes;
  }

  public List<String> getSchemaNames() {
    try {
      return schemaManager.getNames();
    } catch ( HopException e ) {
      return Collections.emptyList();
    }
  }

  public String[] getSchemaNamesArray() {
    List<String> schemas = getSchemaNames();
    return schemas.toArray( new String[ schemas.size() ] );
  }

  public List<PartitionSchema> getSchemas() {
    try {
      return schemaManager.getSerializer().loadAll();
    } catch ( Exception e ) {
      e.printStackTrace( System.out ); // TODO: properly throw exception, don't eat exception like this!!!
      return Collections.emptyList();
    }
  }

  public TransformMeta getTransformMeta() {
    return transformMeta;
  }

  public void updateMethodType( int methodType ) {
    transformMeta.getTransformPartitioningMeta().setMethodType( methodType );
  }

  public void updateMethod( String method ) throws HopPluginException {
    transformMeta.getTransformPartitioningMeta().setMethod( method );
  }

  public void updateSchema( PartitionSchema schema ) {
    if ( schema != null && schema.getName() != null ) {
      transformMeta.getTransformPartitioningMeta().setPartitionSchema( schema );
    }
  }

  public void rollback( TransformMeta before ) throws HopPluginException {
    updateMethod( before.getTransformPartitioningMeta().getMethod() );
    updateMethodType( before.getTransformPartitioningMeta().getMethodType() );
    updateSchema( before.getTransformPartitioningMeta().getPartitionSchema() );
  }

  public TransformMeta getBefore() {
    return before;
  }

  public TransformMeta getAfter() {
    return (TransformMeta) transformMeta.clone();
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }
}
