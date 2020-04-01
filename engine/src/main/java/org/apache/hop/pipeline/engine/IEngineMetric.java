package org.apache.hop.pipeline.engine;

public interface IEngineMetric {
  String getCode();

  String getHeader();

  String getTooltip();

  String getDisplayPriority();

  boolean isNumeric();
}
