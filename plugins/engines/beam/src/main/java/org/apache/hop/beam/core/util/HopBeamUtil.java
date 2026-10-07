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

package org.apache.hop.beam.core.util;

import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HopBeamUtil {
  private HopBeamUtil() {
    // To not be used by accident
  }

  public static String createTargetTupleId(String transformName, String targetTransformName) {
    return transformName + " - TARGET - " + targetTransformName;
  }

  public static String createMainOutputTupleId(String transformName) {
    return transformName + " - OUTPUT";
  }

  public static String createInfoTupleId(String transformName, String infoTransformName) {
    return infoTransformName + " - INFO - " + transformName;
  }

  public static String createMainInputTupleId(String transformName) {
    return transformName + " - INPUT";
  }

  /**
   * Create a copy but don't over allocate objects in the target row.
   *
   * @param hopRow The HopRow to copy
   * @param rowMeta The row metadata to copy use
   * @return A copy of the HopRow given
   */
  public static HopRow copyHopRow(HopRow hopRow, IRowMeta rowMeta) {
    Object[] newRow = new Object[rowMeta.size()];
    System.arraycopy(hopRow.getRow(), 0, newRow, 0, rowMeta.size());
    return new HopRow(newRow);
  }

  /**
   * Rows leaving a Hop transform in Beam are serialized with the HopRowCoder and described
   * downstream by JSON row metadata, which only knows about normal storage. Values kept in lazy
   * (binary string) or indexed storage, such as those of a CSV File Input with lazy conversion, are
   * therefore converted to normal storage here. Values already in normal storage, including real
   * binary data, are passed along untouched.
   *
   * @param rowMeta The row metadata of the transform output, including the storage information
   * @param row The row to convert
   * @return The given row if all values are in normal storage, a converted copy otherwise
   * @throws HopTransformException In case a value can't be converted
   */
  public static Object[] toNormalStorage(IRowMeta rowMeta, Object[] row)
      throws HopTransformException {
    Object[] normalRow = row;
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      if (!valueMeta.isStorageNormal()) {
        if (normalRow == row) {
          normalRow = row.clone();
        }
        try {
          normalRow[i] = valueMeta.convertToNormalStorageType(row[i]);
        } catch (HopValueException e) {
          throw new HopTransformException(
              "Error converting field '" + valueMeta.getName() + "' to normal storage", e);
        }
      }
    }
    return normalRow;
  }

  private static final Object object = new Object();

  public static void loadTransformMetadataFromXml(
      String transformName,
      ITransformMeta iTransformMeta,
      String iTransformXml,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    synchronized (object) {
      Document transformDocument = XmlHandler.loadXmlString(iTransformXml);
      if (transformDocument == null) {
        throw new HopException("Unable to load transform XML document from : " + iTransformXml);
      }
      Node transformNode = XmlHandler.getSubNode(transformDocument, TransformMeta.XML_TAG);
      if (transformNode == null) {
        throw new HopException(
            "Unable to find XML tag " + TransformMeta.XML_TAG + " from : " + iTransformXml);
      }
      try {
        iTransformMeta.loadXml(transformNode, metadataProvider);
      } catch (Exception e) {
        throw new HopException(
            "There was an error loading transform metadata information (loadXml) for transform '"
                + transformName
                + "'",
            e);
      } finally {
        XmlHandlerCache.getInstance().clear();
      }
    }
  }
}
