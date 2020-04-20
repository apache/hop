package org.apache.hop.pipeline.engine;

import java.util.Objects;

public class EngineComponent implements IEngineComponent {
  private String name;
  private int copyNr;
  private String logChannelId;
  private String logText;
  private boolean running;
  private boolean selected;
  private long errors;

  public EngineComponent() {
  }

  public EngineComponent( String name, int copyNr ) {
    this();
    this.name = name;
    this.copyNr = copyNr;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EngineComponent that = (EngineComponent) o;
    return copyNr == that.copyNr &&
      Objects.equals( name, that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name, copyNr );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  @Override public int getCopyNr() {
    return copyNr;
  }

  /**
   * @param copyNr The copyNr to set
   */
  public void setCopyNr( int copyNr ) {
    this.copyNr = copyNr;
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  @Override public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @param logChannelId The logChannelId to set
   */
  public void setLogChannelId( String logChannelId ) {
    this.logChannelId = logChannelId;
  }

  /**
   * Gets logText
   *
   * @return value of logText
   */
  @Override public String getLogText() {
    return logText;
  }

  /**
   * @param logText The logText to set
   */
  public void setLogText( String logText ) {
    this.logText = logText;
  }

  /**
   * Gets running
   *
   * @return value of running
   */
  @Override public boolean isRunning() {
    return running;
  }

  /**
   * @param running The running to set
   */
  public void setRunning( boolean running ) {
    this.running = running;
  }

  /**
   * Gets selected
   *
   * @return value of selected
   */
  @Override public boolean isSelected() {
    return selected;
  }

  /**
   * @param selected The selected to set
   */
  public void setSelected( boolean selected ) {
    this.selected = selected;
  }

  /**
   * Gets errors
   *
   * @return value of errors
   */
  @Override public long getErrors() {
    return errors;
  }

  /**
   * @param errors The errors to set
   */
  public void setErrors( long errors ) {
    this.errors = errors;
  }
}
