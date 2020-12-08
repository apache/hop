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
package org.apache.hop.pipeline.transforms.streamschemamerge;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

import java.util.Collections;
import java.util.HashSet;

/**
 * Takes in RowMetas and find the union of them. Then maps the field of each row to its final destination
 */
public class SchemaMapper {
    IRowMeta row;  // resolved row meta
    int[][] mapping;

    public SchemaMapper(IRowMeta info[]) throws HopPluginException {
        unionMerge(info);
    }

    /**
     * Given RowMetas find the union of all of them. Create a mapping along the way so we know how to move the fields
     * into their appropriate place
     * @param info row metas for the fields to merge
     */
    private void unionMerge(IRowMeta info[]) throws HopPluginException {
        // do set up
        mapping = new int[info.length][];
        IRowMeta base = info[0].clone();
        HashSet<String> fieldNames = new HashSet<>();
        Collections.addAll(fieldNames, base.getFieldNames());

        // do merge
        for (int i = 0; i < info.length; i++) {
            int[] rowMapping = new int[info[i].size()];
            for (int x = 0; x < rowMapping.length; x++) {
                IValueMeta field = info[i].getValueMeta(x);
                String name = field.getName();
                if (!fieldNames.contains(name)) {
                    base.addValueMeta(field);
                    fieldNames.add(name);
                }
                int basePosition = base.indexOfValue(name);
                rowMapping[x] = basePosition;  // update mapping for this field
                // check if we need to change the data type to string
                IValueMeta baseField = base.getValueMeta(basePosition);
                if (baseField.getType() != field.getType()) {
                    IValueMeta updatedField = ValueMetaFactory.cloneValueMeta(baseField, IValueMeta.TYPE_STRING);
                    base.setValueMeta(basePosition, updatedField);
                }

            }
            mapping[i] = rowMapping;  // save the mapping for this rowMeta
        }
        row = base;  // set our master output row
    }

    /**
     * Get mappings for all rows
     * @return mappings from all input rows to the output row format
     */
    public int[][] getMapping() {
        return mapping;
    }

    /**
     * Get master output row
     * @return row meta for union of all output rows
     */
    public IRowMeta getRowMeta() {
        return row;
    }

}
