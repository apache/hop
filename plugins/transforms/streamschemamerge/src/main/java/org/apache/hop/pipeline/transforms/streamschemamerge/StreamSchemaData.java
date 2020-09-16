/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.apache.hop.pipeline.transforms.streamschemamerge;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.List;

/**
 * Holds data objects used in StreamSchema
 */
public class StreamSchemaData extends BaseTransformData implements ITransformData {

	public IRowMeta outputRowMeta, inRowMeta;  // outgoing and incoming row meta

	public StreamSchemaData()
	{
		super();
	}

	public SchemaMapper schemaMapping;  // object that does row mapping

	public List<IStream> infoStreams;  // streams of the incoming steps

	public List<IRowSet> rowSets;  // a list of rowsets that are sending data to this step

	public IRowMeta[] rowMetas;  // a list of row meta information for incoming rows

	public int[][] mapping;  // mappings for all incoming rows

	public int numTransforms, streamNum;  // incoming steps and what stream the current row is from

	public String currentName;  // name of the rowset that sent the current row

	public int[] rowMapping;  // row mapping for the current row

	public String[] TransformNames;  // rowset names for incoming rowsets

	public IRowSet r;  // used for iterating over rowsets
}
	
