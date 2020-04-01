package org.apache.hop.pipeline.engine;

import java.util.Objects;

public class EngineMetric implements IEngineMetric {
  private String code;
  private String header;
  private String tooltip;
  private String displayPriority;
  private boolean numeric;

  public EngineMetric() {
  }

  public EngineMetric( String code, String header, String tooltip, String displayPriority, boolean numeric ) {
    this.code = code;
    this.header = header;
    this.tooltip = tooltip;
    this.displayPriority = displayPriority;
    this.numeric = numeric;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EngineMetric that = (EngineMetric) o;
    return Objects.equals( code, that.code );
  }

  @Override public int hashCode() {
    return Objects.hash( code );
  }

  /**
   * Gets code
   *
   * @return value of code
   */
  @Override public String getCode() {
    return code;
  }

  /**
   * @param code The code to set
   */
  public void setCode( String code ) {
    this.code = code;
  }

  /**
   * Gets header
   *
   * @return value of header
   */
  @Override public String getHeader() {
    return header;
  }

  /**
   * @param header The header to set
   */
  public void setHeader( String header ) {
    this.header = header;
  }

  /**
   * Gets tooltip
   *
   * @return value of tooltip
   */
  @Override public String getTooltip() {
    return tooltip;
  }

  /**
   * @param tooltip The tooltip to set
   */
  public void setTooltip( String tooltip ) {
    this.tooltip = tooltip;
  }

  /**
   * Gets displayPriority
   *
   * @return value of displayPriority
   */
  @Override public String getDisplayPriority() {
    return displayPriority;
  }

  /**
   * @param displayPriority The displayPriority to set
   */
  public void setDisplayPriority( String displayPriority ) {
    this.displayPriority = displayPriority;
  }

  /**
   * Gets numeric
   *
   * @return value of numeric
   */
  @Override public boolean isNumeric() {
    return numeric;
  }

  /**
   * @param numeric The numeric to set
   */
  public void setNumeric( boolean numeric ) {
    this.numeric = numeric;
  }
}
