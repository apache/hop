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

package org.apache.hop.ui.hopgui.perspective;

import java.nio.charset.StandardCharsets;
import org.eclipse.swt.dnd.ByteArrayTransfer;
import org.eclipse.swt.dnd.TransferData;

/**
 * Transfer type for dragging metadata elements (objectKey + name) within the metadata perspective
 * tree or onto the canvas to open them. Payload is a String array: [0] = objectKey, [1] = name.
 */
public class MetadataTransfer extends ByteArrayTransfer {

  public static final MetadataTransfer INSTANCE = new MetadataTransfer();
  private static final String TYPE_NAME =
      "MetadataTransfer.String[] " + System.currentTimeMillis() + ":" + INSTANCE.hashCode();
  private static final int TYPEID = registerType(TYPE_NAME);

  /** Cached payload for same-JVM transfer (drag from tree to tree or tree to tab folder). */
  private String[] metadataPayload;

  private MetadataTransfer() {}

  @Override
  protected int[] getTypeIds() {
    return new int[] {TYPEID};
  }

  @Override
  protected String[] getTypeNames() {
    return new String[] {TYPE_NAME};
  }

  @Override
  public void javaToNative(Object object, TransferData transferData) {
    metadataPayload = (String[]) object;
    if (metadataPayload != null && metadataPayload.length >= 2 && transferData != null) {
      String encoded = metadataPayload[0] + "\n" + metadataPayload[1];
      super.javaToNative(encoded.getBytes(StandardCharsets.UTF_8), transferData);
    }
  }

  @Override
  public Object nativeToJava(TransferData transferData) {
    byte[] bytes = (byte[]) super.nativeToJava(transferData);
    if (bytes == null) {
      return metadataPayload;
    }
    String encoded = new String(bytes, StandardCharsets.UTF_8);
    int idx = encoded.indexOf('\n');
    if (idx >= 0) {
      return new String[] {encoded.substring(0, idx), encoded.substring(idx + 1)};
    }
    return metadataPayload;
  }
}
