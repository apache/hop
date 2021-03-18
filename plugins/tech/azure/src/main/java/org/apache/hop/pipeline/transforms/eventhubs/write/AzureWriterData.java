package org.apache.hop.pipeline.transforms.eventhubs.write;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class AzureWriterData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;

  public ConnectionStringBuilder connectionStringBuilder;
  public ScheduledExecutorService executorService;
  public EventHubClient eventHubClient;
  public long batchSize;
  public int fieldIndex;
  public LinkedList<EventData> list;
}
