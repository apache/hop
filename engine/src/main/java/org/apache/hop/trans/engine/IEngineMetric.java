package org.apache.hop.trans.engine;

public interface IEngineMetric {
  String getCode();
  String getHeader();
  String getTooltip();
  String getDisplayPriority();
  boolean isNumeric();
}
