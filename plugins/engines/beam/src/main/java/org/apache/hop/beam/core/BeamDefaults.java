/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
