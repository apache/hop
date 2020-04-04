/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.singlethreader;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class SingleThreaderData extends BaseTransformData implements ITransformData {
  public Pipeline mappingPipeline;

  public SingleThreadedPipelineExecutor executor;

  public int batchSize;

  public PipelineMeta mappingPipelineMeta;

  public IRowMeta outputRowMeta;
  public RowProducer rowProducer;

  public int batchCount;
  public int batchTime;
  public long startTime;
  public TransformMeta injectTransformMeta;
  public TransformMeta retrieveTransformMeta;
  public List<Object[]> errorBuffer;
  public int lastLogLine;

  public SingleThreaderData() {
    super();
    mappingPipeline = null;

  }

  public Pipeline getMappingPipeline() {
    return mappingPipeline;
  }

  public void setMappingPipeline( Pipeline mappingPipeline ) {
    this.mappingPipeline = mappingPipeline;
  }
}
