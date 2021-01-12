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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;

/**
 * Merge streams from multiple different transforms into a single stream. Unlike most other transforms, this transform does NOT
 * require the incoming rows to have the same RowMeta. Instead, this transform will examine the incoming rows and take the
 * union of the set of all rows passed in. Fields that have the same name will be placed in the same field. The field
 * type will be taken from the first occurrence of a field.
 *
 * Because this transform combines multiple streams with different RowMetas together, it is deemed "not safe" and will fail
 * if you try to run the pipeline with the "Enable Safe Mode checked". Therefore it disables safe mode
 *
 * @author aoverton
 * @since 18-aug-2015
 *
 */

public class StreamSchema extends BaseTransform<StreamSchemaMeta, StreamSchemaData> implements ITransform<StreamSchemaMeta, StreamSchemaData> {

	/**
	 * The constructor should simply pass on its arguments to the parent class.
	 *
	 * @param transformMeta 				transform description
	 * @param data	transform data class
	 * @param copyNr					transform copy
	 * @param pipelineMeta					transformation description
	 * @param pipeline				transformation executing
	 */
	public StreamSchema(TransformMeta transformMeta, StreamSchemaMeta meta, StreamSchemaData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
        super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
        pipeline.setSafeModeEnabled(false);  // safe mode is incompatible with this transform
	}

	/**
	 * Initialize data structures that we have information for at init time
	 *
	 * @return true if initialization completed successfully, false if there was an error
	 *
	 */
	public boolean init() {
		// Casting to transform-specific implementation classes is safe

		data.infoStreams = meta.getTransformIOMeta().getInfoStreams();
		data.numTransforms = data.infoStreams.size();
		data.rowMetas = new IRowMeta[data.numTransforms];
		data.rowSets = new ArrayList<>();
        data.TransformNames = new String[data.numTransforms];

		return super.init();
	}

	/**
	 * For each row, create a new output row in the model of the master output row and copy the data values in to the
     * appropriate indexes
	 *
	 * @return true to indicate that the function should be called again, false if the transform is done
	 */
	public boolean processRow() throws HopException {

        /*
         * Code in first method is responsible for finishing the initialization that we couldn't do earlier
         */
		if (first) {
			first = false;

            for (int i = 0; i < data.infoStreams.size(); i++) {
                data.r = findInputRowSet(data.infoStreams.get(i).getTransformName());
                data.rowSets.add(data.r);
                data.TransformNames[i] = data.r.getName();
                // Avoids race condition. Row metas are not available until the previous transforms have called
                // putRowWait at least once
                while (data.rowMetas[i] == null && !isStopped()) {
                    data.rowMetas[i] = data.r.getRowMeta();
                }
            }

			data.schemaMapping = new SchemaMapper(data.rowMetas);  // creates mapping and master output row
			data.mapping = data.schemaMapping.getMapping();
			data.outputRowMeta = data.schemaMapping.getRowMeta();
			setInputRowSets(data.rowSets);  // set the order of the inputrowsets to match the order we've defined
            if (isDetailed()) {
                logDetailed("Finished generating mapping");
            }

		}

		Object[] incomingRow = getRow();  // get the next available row

		// if no more rows are expected, indicate transform is finished and processRow() should not be called again
		if (incomingRow == null){
			setOutputDone();
			return false;
		}

        // get the name of the transform that the current rowset is coming from
		data.currentName = getInputRowSets().get(getCurrentInputRowSetNr()).getName();
        // because rowsets are removed from the list of rowsets once they're exhausted (in the getRow() method) we
        // need to use the name to find the proper index for our lookups later
		for (int i = 0; i < data.TransformNames.length; i++) {
			if (data.TransformNames[i].equals(data.currentName)) {
				data.streamNum = i;
				break;
			}
		}
        if (isRowLevel()) {
            logRowlevel(String.format("Current row from %s. This maps to stream number %d", data.currentName,
                    data.streamNum));
        }

        // create a new (empty) output row in the model of the master outputer row
		Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

		data.rowMapping = data.mapping[data.streamNum];  // set appropriate row mapping
		data.inRowMeta = data.rowMetas[data.streamNum];  // set appropriate meta for incoming row
		for (int j = 0; j < data.inRowMeta.size(); j++) {
            int newPos = data.rowMapping[j];
			outputRow[newPos] = incomingRow[j];  // map a fields old position to its new position
		}

		// put the row to the output row stream
		putRow(data.outputRowMeta, outputRow);

		// log progress if it is time to to so
		if (checkFeedback(getLinesRead())) {
			logBasic("Linenr " + getLinesRead()); // Some basic logging
		}

		// indicate that processRow() should be called again
		return true;
	}

    /**
     * Clear transforms from transform data
     */
    public void dispose() {

        data.outputRowMeta = null;
        data.inRowMeta = null;
        data.schemaMapping = null;
        data.infoStreams = null;
        data.rowSets = null;
        data.rowMetas = null;
        data.mapping = null;
        data.currentName = null;
        data.rowMapping = null;
        data.TransformNames = null;
        data.r = null;

        super.dispose();
    }

}
