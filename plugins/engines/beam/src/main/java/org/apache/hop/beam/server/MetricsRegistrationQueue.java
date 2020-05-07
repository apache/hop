package org.apache.hop.beam.server;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hop.core.logging.LogChannel;

public class MetricsRegistrationQueue {
  private static MetricsRegistrationQueue registrationQueue;
  
  private Queue<BeamMetricsEntry> entryQueue;
  
  public synchronized static MetricsRegistrationQueue getInstance() {
    if (registrationQueue==null) {
      registrationQueue = new MetricsRegistrationQueue();
    }
    return registrationQueue;
  }
  
  private MetricsRegistrationQueue() {
    entryQueue = new ConcurrentLinkedQueue<BeamMetricsEntry>();
  }
  
  public void addNodeRegistryEntry( BeamMetricsEntry entry) {
    entryQueue.add(entry);
    LogChannel.GENERAL.logDetailed("Beam Metrics update received for job id : "+entry.getContainerObjectId()+", pipeline="+entry.getPipelineName());
  }

  public BeamMetricsEntry pollEntry() {
    return entryQueue.poll();
  }
}
