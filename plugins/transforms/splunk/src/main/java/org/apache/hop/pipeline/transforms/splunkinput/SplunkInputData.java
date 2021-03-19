package org.apache.hop.pipeline.transforms.splunkinput;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.hop.splunk.SplunkConnection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.InputStream;

public class SplunkInputData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public SplunkConnection splunkConnection;
  public int[] fieldIndexes;
  public String query;

  public ServiceArgs serviceArgs;
  public Service service;
  public InputStream eventsStream;
}
