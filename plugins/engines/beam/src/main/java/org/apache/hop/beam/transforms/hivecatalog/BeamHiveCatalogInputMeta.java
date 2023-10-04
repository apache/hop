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

package org.apache.hop.beam.transforms.hivecatalog;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamInputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.beam.transforms.io.BeamInputDialog;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
        id = "BeamHiveCatalogInput",
        image = "beam-output.svg",
        name = "i18n::BeamHiveCatalogInputDialog.DialogTitle",
        description = "i18n::BeamHiveCatalogInputDialog.Description",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        keywords = "i18n::BeamHiveCatalogInputDialog.keyword",
        documentationUrl = "/pipeline/transforms/beamoutput.html")
public class BeamHiveCatalogInputMeta extends BaseTransformMeta<BeamHiveCatalogInput, BeamHiveCatalogInputData> implements IBeamPipelineTransformHandler {
    public BeamHiveCatalogInputMeta() {}

    @HopMetadataProperty(key = "hive_metastore_uris")
    private String hiveMetastoreUris;

    @HopMetadataProperty(key = "hive_metastore_databese")
    private String hiveMetastoreDatabase;

    @HopMetadataProperty(key = "hive_metastore_table")
    private String hiveMetastoreTable;


    @Override
    public String getDialogClassName() {
        return BeamHiveCatalogInputDialog.class.getName();
    }

    @Override
    public boolean isInput() {
        return true;
    }

    @Override
    public boolean isOutput() {
        return false;
    }

    @Override
    public void handleTransform(
            ILogChannel log,
            IVariables variables,
            String runConfigurationName,
            IBeamPipelineEngineRunConfiguration runConfiguration,
            String dataSamplersJson,
            IHopMetadataProvider metadataProvider,
            PipelineMeta pipelineMeta,
            TransformMeta transformMeta,
            Map<String, PCollection<HopRow>> transformCollectionMap,
            org.apache.beam.sdk.Pipeline pipeline,
            IRowMeta rowMeta,
            List<TransformMeta> previousTransforms,
            PCollection<HopRow> input,
            String parentLogChannelId)
            throws HopException {
    }

    public String getHiveMetastoreUris() {
        return hiveMetastoreUris;
    }

    public void setHiveMetastoreUris(String hiveMetastoreUris) {
        this.hiveMetastoreUris = hiveMetastoreUris;
    }

    public String getHiveMetastoreDatabase() {
        return hiveMetastoreDatabase;
    }

    public void setHiveMetastoreDatabase(String hiveMetastoreDatabase) {
        this.hiveMetastoreDatabase = hiveMetastoreDatabase;
    }

    public String getHiveMetastoreTable() {
        return hiveMetastoreTable;
    }

    public void setHiveMetastoreTable(String hiveMetastoreTable) {
        this.hiveMetastoreTable = hiveMetastoreTable;
    }
}
