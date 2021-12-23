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
 *
 */

package org.apache.hop.neo4j.transforms.gencsv;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.neo4j.core.data.*;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateCsv extends BaseTransform<GenerateCsvMeta, GenerateCsvData>
    implements ITransform<GenerateCsvMeta, GenerateCsvData> {

  /**
   * This is the base transform that forms that basis for all transform. You can derive from this
   * class to implement your own transform.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta the transform specific metadata,
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The TransInfo of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transform.
   */
  public GenerateCsv(
      TransformMeta transformMeta,
      GenerateCsvMeta meta,
      GenerateCsvData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      // Write data in the buffer to CSV files...
      //
      writeBufferToCsv();

      // Close all files and pass the filenames to the next transform.
      // These are stored in the fileMap
      //
      if (data.fileMap != null) {
        for (CsvFile csvFile : data.fileMap.values()) {

          try {
            csvFile.closeFile();
          } catch (Exception e) {
            setErrors(1L);
            log.logError("Error flushing/closing file '" + csvFile.getFilename() + "'", e);
          }

          Object[] nodeFileRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
          nodeFileRow[0] = csvFile.getShortFilename();
          nodeFileRow[1] = csvFile.getFileType();
          putRow(data.outputRowMeta, nodeFileRow);
        }
      }

      if (getErrors() > 0) {
        stopAll();
      }

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.fileMap = new HashMap<>();

      data.graphFieldIndex = getInputRowMeta().indexOfValue(meta.getGraphFieldName());
      if (data.graphFieldIndex < 0) {
        throw new HopException(
            "Unable to find graph field " + meta.getGraphFieldName() + "' in the transform input");
      }
      if (getInputRowMeta().getValueMeta(data.graphFieldIndex).getType()
          != ValueMetaGraph.TYPE_GRAPH) {
        throw new HopException("Field " + meta.getGraphFieldName() + "' needs to be of type Graph");
      }

      if (meta.getUniquenessStrategy() != UniquenessStrategy.None) {
        data.indexedGraphData =
            new IndexedGraphData(meta.getUniquenessStrategy(), meta.getUniquenessStrategy());
      } else {
        data.indexedGraphData = null;
      }
      data.filesPrefix = resolve(meta.getFilesPrefix());

      data.filenameField = resolve(meta.getFilenameField());
      data.fileTypeField = resolve(meta.getFileTypeField());

      data.baseFolder = resolve(meta.getBaseFolder());
      if (!data.baseFolder.endsWith(File.separator)) {
        data.baseFolder += File.separator;
      }

      data.importFolder = data.baseFolder + "import/";
    }

    if (meta.getUniquenessStrategy() != UniquenessStrategy.None) {

      // Add the row to the internal buffer...
      //
      addRowToBuffer(getInputRowMeta(), row);

    } else {

      // Get the graph data
      //
      ValueMetaGraph valueMetaGraph =
          (ValueMetaGraph) getInputRowMeta().getValueMeta(data.graphFieldIndex);
      GraphData graphData = valueMetaGraph.getGraphData(row[data.graphFieldIndex]);

      // Now we're going to build a map between the input graph data property sets and the file the
      // data will end up in.
      // This is stored in data.fileMap
      //
      if (data.fileMap == null) {
        data.fileMap = new HashMap<>();
      }

      // Process all the nodes and relationships in the graph data...
      // Write it all to CSV file
      //
      writeGraphDataToCsvFiles(graphData);
    }

    // Don't pass the rows to the next transform, only the file names at the end.
    //
    return true;
  }

  protected void writeGraphDataToCsvFiles(GraphData graphData) throws HopException {

    if (graphData == null) {
      return;
    }

    // First update the CSV files...
    //
    writeNodesToCsvFiles(graphData);

    // Now do the relationships...
    //
    writeRelationshipsToCsvFiles(graphData);
  }

  protected void writeNodesToCsvFiles(GraphData graphData) throws HopException {
    for (GraphNodeData nodeData : graphData.getNodes()) {

      // The files we're generating for import need to have the same sets of properties
      // This set of properties is the same for every transform generating the graph data types.
      // So we can use the name of the property transform given by the transform in combination with
      // the
      // pipeline and transform name.
      //
      String propertySetKey =
          GenerateCsvData.getPropertySetKey(
              graphData.getSourcePipelineName(),
              graphData.getSourceTransformName(),
              nodeData.getPropertySetId());

      // See if we have this set already?
      //
      CsvFile csvFile = data.fileMap.get(propertySetKey);
      if (csvFile == null) {
        // Create a new file and write the header for the node...
        //
        String filename = calculateNodeFilename(propertySetKey);
        String shortFilename = calculateNodeShortFilename(propertySetKey);
        csvFile = new CsvFile(filename, shortFilename, "Nodes");
        data.fileMap.put(propertySetKey, csvFile);

        try {
          csvFile.openFile();
        } catch (Exception e) {
          throw new HopException(
              "Unable to create nodes CSV file '" + csvFile.getFilename() + "'", e);
        }

        // Calculate the unique list of node properties
        //
        List<GraphPropertyData> properties = nodeData.getProperties();
        for (int i = 0; i < properties.size(); i++) {
          csvFile
              .getPropsList()
              .add(new IdType(properties.get(i).getId(), properties.get(i).getType()));
        }
        for (int i = 0; i < properties.size(); i++) {
          csvFile.getPropsIndexes().put(properties.get(i).getId(), i);
        }
        csvFile.setIdFieldName("id");
        for (GraphPropertyData property : properties) {
          if (property.isPrimary()) {
            csvFile.setIdFieldName(property.getId());
            break; // First ID found
          }
        }

        try {
          writeNodeCsvHeader(
              csvFile.getOutputStream(), csvFile.getPropsList(), csvFile.getIdFieldName());
        } catch (Exception e) {
          throw new HopException(
              "Unable to write node header to file '" + csvFile.getFilename() + "'", e);
        }
      }

      // Write a node data row to the CSV file...
      //
      try {
        writeNodeCsvRows(
            csvFile.getOutputStream(),
            Arrays.asList(nodeData),
            csvFile.getPropsList(),
            csvFile.getPropsIndexes(),
            csvFile.getIdFieldName());
      } catch (Exception e) {
        throw new HopException(
            "Unable to write node header to file '" + csvFile.getFilename() + "'", e);
      }
    }
  }

  protected void writeRelationshipsToCsvFiles(GraphData graphData) throws HopException {
    for (GraphRelationshipData relationshipData : graphData.getRelationships()) {

      // The files we're generating for import need to have the same sets of properties
      // This set of properties is the same for every transform generating the graph data types.
      // So we can use the name of the property transform given by the transform in combination with
      // the
      // pipeline and transform name.
      //
      String propertySetKey =
          GenerateCsvData.getPropertySetKey(
              graphData.getSourcePipelineName(),
              graphData.getSourceTransformName(),
              relationshipData.getPropertySetId());

      // See if we have this set already?
      //
      CsvFile csvFile = data.fileMap.get(propertySetKey);
      if (csvFile == null) {
        // Create a new file and write the header for the node...
        //
        String filename = calculateRelatiohshipsFilename(propertySetKey);
        String shortFilename = calculateRelatiohshipsShortFilename(propertySetKey);
        csvFile = new CsvFile(filename, shortFilename, "Relationships");
        data.fileMap.put(propertySetKey, csvFile);

        try {
          csvFile.openFile();
        } catch (Exception e) {
          throw new HopException(
              "Unable to create relationships CSV file '" + csvFile.getFilename() + "'", e);
        }

        // Calculate the unique list of node properties
        //
        List<GraphPropertyData> properties = relationshipData.getProperties();
        for (int i = 0; i < properties.size(); i++) {
          csvFile
              .getPropsList()
              .add(new IdType(properties.get(i).getId(), properties.get(i).getType()));
        }
        for (int i = 0; i < properties.size(); i++) {
          csvFile.getPropsIndexes().put(properties.get(i).getId(), i);
        }
        csvFile.setIdFieldName("id");
        for (GraphPropertyData property : properties) {
          if (property.isPrimary()) {
            csvFile.setIdFieldName(property.getId());
            break; // First ID found
          }
        }

        try {
          writeRelsCsvHeader(
              csvFile.getOutputStream(), csvFile.getPropsList(), csvFile.getPropsIndexes());
        } catch (Exception e) {
          throw new HopException(
              "Unable to write relationships header to file '" + csvFile.getFilename() + "'", e);
        }
      }

      // Write a relationships data row to the CSV file...
      //
      try {
        writeRelsCsvRows(
            csvFile.getOutputStream(),
            Arrays.asList(relationshipData),
            csvFile.getPropsList(),
            csvFile.getPropsIndexes());
      } catch (Exception e) {
        throw new HopException(
            "Unable to write relationships header to file '" + csvFile.getFilename() + "'", e);
      }
    }
  }

  protected void writeBufferToCsv() throws HopException {

    try {

      if (meta.getUniquenessStrategy() != UniquenessStrategy.None) {

        // Same logic
        //
        writeGraphDataToCsvFiles(data.indexedGraphData);
      }

    } catch (Exception e) {
      throw new HopException("Unable to generate CSV data for Neo4j import", e);
    }
  }

  private String calculateNodeShortFilename(String propertySetKey) {
    return "import/"
        + Const.NVL(data.filesPrefix + "-", "")
        + "nodes-"
        + propertySetKey
        + "-"
        + resolve("${" + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR + "}")
        + ".csv";
  }

  private String calculateNodeFilename(String propertySetKey) {
    return data.importFolder
        + Const.NVL(data.filesPrefix + "-", "")
        + "nodes-"
        + propertySetKey
        + "-"
        + resolve("${" + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR + "}")
        + ".csv";
  }

  private String calculateRelatiohshipsShortFilename(String propertySetKey) {
    return "import/"
        + Const.NVL(data.filesPrefix + "-", "")
        + "rels-"
        + propertySetKey
        + "-"
        + resolve("${" + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR + "}")
        + ".csv";
  }

  private String calculateRelatiohshipsFilename(String propertySetKey) {
    return data.importFolder
        + Const.NVL(data.filesPrefix + "-", "")
        + "rels-"
        + propertySetKey
        + "-"
        + resolve("${" + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR + "}")
        + ".csv";
  }

  private void writeNodeCsvHeader(OutputStream os, List<IdType> props, String idFieldName)
      throws HopException, IOException {
    // Write the values to the file...
    //

    StringBuffer header = new StringBuffer();

    // The id...
    //
    header.append(idFieldName).append(":ID");

    for (IdType prop : props) {

      if (!prop.getId().equals(idFieldName)) {
        header.append(",");

        header.append(prop.getId());
        if (prop.getType() == null) {
          throw new HopException(
              "This transform doesn't support importing data type '"
                  + prop.getType().name()
                  + "' yet.");
        }
        header.append(":").append(prop.getType().getImportType());
      }
    }
    header.append(",:LABEL");
    header.append(Const.CR);

    System.out.println("NODES HEADER: '" + header + "'");
    os.write(header.toString().getBytes("UTF-8"));
  }

  private void writeNodeCsvRows(
      OutputStream os,
      List<GraphNodeData> nodes,
      List<IdType> props,
      Map<String, Integer> propertyIndexes,
      String idFieldName)
      throws IOException {
    // Now we serialize the data...
    //
    for (GraphNodeData node : nodes) {

      StringBuffer row = new StringBuffer();

      // Get the properties in the right order of list props...
      //
      GraphPropertyData[] sortedProperties = new GraphPropertyData[props.size()];
      for (GraphPropertyData prop : node.getProperties()) {
        int index = propertyIndexes.get(prop.getId());
        sortedProperties[index] = prop;
      }

      // First write the index field...
      //
      boolean indexFound = false;
      for (int i = 0; i < sortedProperties.length; i++) {
        GraphPropertyData prop = sortedProperties[i];
        if (prop.isPrimary()) {
          if (prop.getType() == GraphPropertyDataType.String) {
            row.append('"').append(prop.toString()).append('"');
          } else {
            row.append(prop.toString());
          }
          indexFound = true;
          break;
        }
      }
      // No index field? Take the node index
      //
      if (!indexFound) {
        row.append('"').append(GraphPropertyData.escapeString(node.getId())).append('"');
      }

      // Now write the other properties to the file
      //
      for (int i = 0; i < sortedProperties.length; i++) {
        GraphPropertyData prop = sortedProperties[i];
        if (!prop.isPrimary()) {
          row.append(",");
          if (prop != null) {
            if (prop.getType() == GraphPropertyDataType.String) {
              row.append('"').append(prop.toString()).append('"');
            } else {
              row.append(prop.toString());
            }
          }
        }
      }

      // Now write the labels for this node
      //
      for (int i = 0; i < node.getLabels().size(); i++) {
        if (i == 0) {
          row.append(",");
        } else {
          row.append(";");
        }
        String label = node.getLabels().get(i);
        row.append(label);
      }
      row.append(Const.CR);

      // Write it out
      //
      os.write(row.toString().getBytes("UTF-8"));
    }
  }

  private void writeRelsCsvRows(
      OutputStream os,
      List<GraphRelationshipData> relationships,
      List<IdType> props,
      Map<String, Integer> propertyIndexes)
      throws IOException {

    // Now write the actual rows of data...
    //
    for (GraphRelationshipData relationship : relationships) {

      StringBuffer row = new StringBuffer();
      row.append('"')
          .append(GraphPropertyData.escapeString(relationship.getSourceNodeId()))
          .append('"');

      // Get the properties in the right order of list props...
      //
      GraphPropertyData[] sortedProperties = new GraphPropertyData[props.size()];
      for (GraphPropertyData prop : relationship.getProperties()) {
        int index = propertyIndexes.get(prop.getId());
        sortedProperties[index] = prop;
      }

      // Now write the list of properties to the file starting with the ID.
      //
      for (int i = 0; i < sortedProperties.length; i++) {
        GraphPropertyData prop = sortedProperties[i];
        row.append(",");
        if (prop != null) {

          if (prop.getType() == GraphPropertyDataType.String) {
            row.append('"').append(prop.toString()).append('"');
          } else {
            row.append(prop.toString());
          }
        }
      }

      row.append(",")
          .append('"')
          .append(GraphPropertyData.escapeString(relationship.getTargetNodeId()))
          .append('"');

      // Now write the labels for this node
      //
      row.append(",").append(relationship.getLabel());
      row.append(Const.CR);

      // Write it out
      //
      os.write(row.toString().getBytes("UTF-8"));
    }
  }

  private void writeRelsCsvHeader(
      OutputStream os, List<IdType> props, Map<String, Integer> propertyIndexes)
      throws HopException, IOException {
    StringBuffer header = new StringBuffer();

    header.append(":START_ID");

    for (IdType prop : props) {

      header.append(",");

      header.append(prop.getId());
      if (prop.getType() == null) {
        throw new HopException(
            "This transform doesn't support importing data type '"
                + prop.getType().name()
                + "' yet.");
      }
      header.append(":").append(prop.getType().getImportType());

      GraphPropertyDataType type = prop.getType();
    }

    header.append(",:END_ID,:TYPE").append(Const.CR);

    os.write(header.toString().getBytes("UTF-8"));
  }

  protected void addRowToBuffer(IRowMeta inputRowMeta, Object[] row) throws HopException {

    try {

      ValueMetaGraph valueMetaGraph =
          (ValueMetaGraph) inputRowMeta.getValueMeta(data.graphFieldIndex);
      GraphData graphData = valueMetaGraph.getGraphData(row[data.graphFieldIndex]);

      // Add all nodes and relationship to the indexed graph...
      //
      for (GraphNodeData node : graphData.getNodes()) {

        // Copy the data and calculate a unique property set ID
        //
        GraphNodeData nodeCopy = new GraphNodeData(node);
        nodeCopy.setPropertySetId(
            graphData.getSourcePipelineName()
                + "-"
                + graphData.getSourceTransformName()
                + "-"
                + node.getPropertySetId());

        data.indexedGraphData.addAndIndexNode(nodeCopy);
      }

      for (GraphRelationshipData relationship : graphData.getRelationships()) {

        // Copy the data and calculate a unique property set ID
        //
        GraphRelationshipData relationshipCopy = new GraphRelationshipData(relationship);
        relationshipCopy.setPropertySetId(
            graphData.getSourcePipelineName()
                + "-"
                + graphData.getSourceTransformName()
                + "-"
                + relationship.getPropertySetId());

        data.indexedGraphData.addAndIndexRelationship(relationshipCopy);
      }

    } catch (Exception e) {
      throw new HopException("Error adding row to gencsv buffer", e);
    }
  }
}
