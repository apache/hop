package org.apache.hop.pipeline.transforms.eventhubs.listen;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class AzureListenerData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;

  public ConnectionStringBuilder connectionStringBuilder;
  public ScheduledExecutorService executorService;
  public EventHubClient eventHubClient;
  public int batchSize;
  public int prefetchSize;
  public LinkedList<EventData> list;
  public String outputField;
  public String partitionIdField;
  public String offsetField;
  public String sequenceNumberField;
  public String hostField;
  public String enqueuedTimeField;

  public PipelineMeta sttPipelineMeta;
  public Pipeline sttPipeline;
  public SingleThreadedPipelineExecutor sttExecutor;
  public boolean stt = false;
  public RowProducer sttRowProducer;
  public long sttMaxWaitTime;
}
