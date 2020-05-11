package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public interface BeamStepHandler {

  void handleStep( ILogChannel log,
                   TransformMeta transformMeta,
                   Map<String, PCollection<HopRow>> stepCollectionMap,
                   Pipeline pipeline,
                   IRowMeta rowMeta,
                   List<TransformMeta> previousSteps,
                   PCollection<HopRow> input
  ) throws HopException;

  boolean isInput();

  boolean isOutput();
}
