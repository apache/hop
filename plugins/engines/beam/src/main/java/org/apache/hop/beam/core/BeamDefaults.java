package org.apache.hop.beam.core;

public class BeamDefaults {
  public static final String PUBSUB_MESSAGE_TYPE_AVROS    = "Avros";
  public static final String PUBSUB_MESSAGE_TYPE_PROTOBUF = "protobuf";
  public static final String PUBSUB_MESSAGE_TYPE_STRING   = "String";
  public static final String PUBSUB_MESSAGE_TYPE_MESSAGE  = "PubsubMessage";

  public static final String[] PUBSUB_MESSAGE_TYPES = new String[] {
    // PUBSUB_MESSAGE_TYPE_AVROS,
    // PUBSUB_MESSAGE_TYPE_PROTOBUF,
    PUBSUB_MESSAGE_TYPE_STRING,
    PUBSUB_MESSAGE_TYPE_MESSAGE,
  };


  public static final String WINDOW_TYPE_FIXED    = "Fixed";
  public static final String WINDOW_TYPE_SLIDING = "Sliding";
  public static final String WINDOW_TYPE_SESSION = "Session";
  public static final String WINDOW_TYPE_GLOBAL = "Global";

  public static final String[] WINDOW_TYPES = new String[] {
    WINDOW_TYPE_FIXED,
    WINDOW_TYPE_SLIDING,
    WINDOW_TYPE_SESSION,
    WINDOW_TYPE_GLOBAL
  };

}
