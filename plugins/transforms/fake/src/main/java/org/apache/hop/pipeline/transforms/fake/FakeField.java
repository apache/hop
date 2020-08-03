package org.apache.hop.pipeline.transforms.fake;

import org.apache.commons.lang.StringUtils;

public class FakeField {
  private String name;
  private String type;
  private String topic;

  public FakeField() {
  }

  public FakeField( String name, String type, String topic ) {
    this.name = name;
    this.type = type;
    this.topic = topic;
  }

  public FakeField( FakeField f ) {
    this.name = f.name;
    this.type = f.type;
    this.topic = f.topic;
  }

  public boolean isValid() {
    return ( StringUtils.isNotEmpty( name ) && StringUtils.isNotEmpty( type ) && StringUtils.isNotEmpty( topic ) );
  }


  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( String type ) {
    this.type = type;
  }

  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }
}
