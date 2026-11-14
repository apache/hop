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

package org.apache.hop.pipeline;

import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.PartitionerPlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.w3c.dom.Node;

/**
 * Rows with the same values in the partitioning fields go to the same partition. Unlike the Mod
 * partitioner it takes several fields, such as a customer and a date. Which partition a key gets is
 * the engine's business: Spark and Beam hash with their own functions, the local engine with the
 * value hash codes.
 */
@PartitionerPlugin(
    id = HashPartitioner.ID,
    name = "Hash on fields",
    description = "Rows with the same values in the fields go to the same partition")
@GuiPlugin
@Getter
@Setter
public class HashPartitioner extends BasePartitioner {
  public static final String ID = "HashPartitioner";
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "HashPartitioner-PluginSpecific";
  public static final String WIDGET_FIELDS = "fields";

  private static final String TAG_FIELDS = "fields";
  private static final String TAG_FIELD = "field";
  private static final String TAG_NAME = "name";

  @GuiWidgetElement(
      id = WIDGET_FIELDS,
      order = "0100",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TABLE,
      label = "i18n::HashPartitioner.Fields.Label",
      toolTip = "i18n::HashPartitioner.Fields.ToolTip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HashPartitioner.Group.Fields",
      tableRows = 6)
  @HopMetadataProperty(groupKey = TAG_FIELDS, key = TAG_FIELD)
  private List<HashPartitionField> fields = new ArrayList<>();

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private transient int[] fieldIndexes;

  public HashPartitioner() {
    super();
    setId(ID);
  }

  /** The names of the partitioning fields, without the empty rows of the table. */
  public List<String> getFieldNames() {
    List<String> names = new ArrayList<>();
    for (HashPartitionField field : fields) {
      if (field != null && field.getName() != null && !field.getName().isBlank()) {
        names.add(field.getName());
      }
    }
    return names;
  }

  @Override
  public IPartitioner getInstance() {
    IPartitioner partitioner = new HashPartitioner();
    partitioner.setId(getId());
    partitioner.setDescription(getDescription());
    return partitioner;
  }

  @Override
  public HashPartitioner clone() {
    HashPartitioner partitioner = (HashPartitioner) super.clone();
    partitioner.fields = new ArrayList<>();
    for (HashPartitionField field : fields) {
      partitioner.fields.add(new HashPartitionField(field.getName()));
    }
    return partitioner;
  }

  @Override
  public String getDialogClassName() {
    return "org.apache.hop.ui.pipeline.dialog.HashPartitionerDialog";
  }

  @Override
  public int getPartition(IVariables variables, IRowMeta rowMeta, Object[] row)
      throws HopException {
    if (rowMeta == null) {
      throw new HopException(
          "No row metadata was provided and so a partition can't be calculated on fields "
              + getFieldNames()
              + " using a hash partitioner");
    }
    init(variables, rowMeta);
    if (fieldIndexes == null) {
      List<String> names = getFieldNames();
      if (names.isEmpty()) {
        throw new HopTransformException("The hash partitioner has no fields to partition on");
      }
      int[] indexes = new int[names.size()];
      for (int i = 0; i < indexes.length; i++) {
        indexes[i] = rowMeta.indexOfValue(names.get(i));
        if (indexes[i] < 0) {
          throw new HopTransformException(
              "Unable to find partitioning field name ["
                  + names.get(i)
                  + "] in the output row..."
                  + rowMeta);
        }
      }
      fieldIndexes = indexes;
    }
    int hash = 1;
    for (int index : fieldIndexes) {
      hash = 31 * hash + rowMeta.getValueMeta(index).hashCode(row[index]);
    }
    return Math.floorMod(hash, nrPartitions);
  }

  @Override
  public String getDescription() {
    List<String> names = getFieldNames();
    return names.isEmpty() ? "Hash partitioner" : "Hash partitioner" + names;
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder(150);
    xml.append("           ").append(XmlHandler.openTag(TAG_FIELDS));
    for (String name : getFieldNames()) {
      xml.append(XmlHandler.openTag(TAG_FIELD))
          .append(XmlHandler.addTagValue(TAG_NAME, name, false))
          .append(XmlHandler.closeTag(TAG_FIELD));
    }
    xml.append(XmlHandler.closeTag(TAG_FIELDS));
    return xml.toString();
  }

  @Override
  public void loadXml(Node partitioningMethodNode) throws HopXmlException {
    fields = new ArrayList<>();
    fieldIndexes = null;
    Node fieldsNode = XmlHandler.getSubNode(partitioningMethodNode, TAG_FIELDS);
    for (Node fieldNode : XmlHandler.getNodes(fieldsNode, TAG_FIELD)) {
      fields.add(new HashPartitionField(XmlHandler.getTagValue(fieldNode, TAG_NAME)));
    }
  }
}
