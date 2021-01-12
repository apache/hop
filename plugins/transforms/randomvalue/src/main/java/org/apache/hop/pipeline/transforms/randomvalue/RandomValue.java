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
package org.apache.hop.pipeline.transforms.randomvalue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Uuid4Util;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

/**
 * Get random value.
 *
 * @author Matt, Samatar
 * @since 8-8-2008
 */
public class RandomValue extends BaseTransform<RandomValueMeta, RandomValueData>
    implements ITransform<RandomValueMeta, RandomValueData> {

  private static final Class<?> PKG = RandomValueMeta.class; // For Translator

  public RandomValue(
      TransformMeta transformMeta,
      RandomValueMeta meta,
      RandomValueData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private Object[] getRandomValue(IRowMeta inputRowMeta, Object[] inputRowData) {
    Object[] row = new Object[data.outputRowMeta.size()];
    for (int i = 0; i < inputRowMeta.size(); i++) {
      row[i] = inputRowData[i]; // no data is changed, clone is not
      // needed here.
    }

    for (int i = 0, index = inputRowMeta.size(); i < meta.getFieldName().length; i++, index++) {
      switch (meta.getFieldType()[i]) {
        case RandomValueMeta.TYPE_RANDOM_NUMBER:
          row[index] = data.randomgen.nextDouble();
          break;
        case RandomValueMeta.TYPE_RANDOM_INTEGER:
          row[index] = Long.valueOf(data.randomgen.nextInt());
          break;
        case RandomValueMeta.TYPE_RANDOM_STRING:
          // TODO Math.abs(Long.MIN_VALUE) == Long.MIN_VALUE --> Don't expect Math.abs always return
          // positive.
          row[index] = Long.toString(Math.abs(data.randomgen.nextLong()), 32);
          break;
        case RandomValueMeta.TYPE_RANDOM_UUID:
          row[index] = UUID.randomUUID().toString();
          break;
        case RandomValueMeta.TYPE_RANDOM_UUID4:
          row[index] = data.u4.getUUID4AsString();
          break;
        case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACMD5", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACSHA1", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        default:
          break;
      }
    }

    return row;
  }

  private String generateRandomMACHash(int algorithm) throws Exception {
    // Generates a secret key
    SecretKey sk = null;
    switch (algorithm) {
      case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
        sk = data.keyGenHmacMD5.generateKey();
        break;
      case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
        sk = data.keyGenHmacSHA1.generateKey();
        break;
      default:
        break;
    }

    if (sk == null) {
      throw new HopException(BaseMessages.getString(PKG, "RandomValue.Log.SecretKeyNull"));
    }

    // Create a MAC object using HMAC and initialize with key
    Mac mac = Mac.getInstance(sk.getAlgorithm());
    mac.init(sk);
    // digest
    byte[] hashCode = mac.doFinal();
    StringBuilder encoded = new StringBuilder();
    for (int i = 0; i < hashCode.length; i++) {
      String b = Integer.toHexString(hashCode[i]);
      if (b.length() == 1) {
        b = "0" + b;
      }
      encoded.append(b.substring(b.length() - 2));
    }

    return encoded.toString();
  }

  public boolean processRow() throws HopException {
    Object[] row;
    if (data.readsRows) {
      row = getRow();
      if (row == null) {
        setOutputDone();
        return false;
      }

      if (first) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }
    } else {
      row = new Object[] {}; // empty row
      incrementLinesRead();

      if (first) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }
    }

    IRowMeta imeta = getInputRowMeta();
    if (imeta == null) {
      imeta = new RowMeta();
      this.setInputRowMeta(imeta);
    }

    row = getRandomValue(imeta, row);

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG, "RandomValue.Log.ValueReturned", data.outputRowMeta.getString(row)));
    }

    putRow(data.outputRowMeta, row);

    if (!data.readsRows) { // Just one row and then stop!

      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init() {

    if (super.init()) {
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms(getTransformMeta());

      if (previous != null && !previous.isEmpty()) {
        data.readsRows = true;
      }
      boolean genHmacMD5 = false;
      boolean genHmacSHA1 = false;
      boolean uuid4 = false;

      for (int i = 0; i < meta.getFieldName().length; i++) {
        switch (meta.getFieldType()[i]) {
          case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
            genHmacMD5 = true;
            break;
          case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
            genHmacSHA1 = true;
            break;
          case RandomValueMeta.TYPE_RANDOM_UUID4:
            uuid4 = true;
            break;
          default:
            break;
        }
      }
      if (genHmacMD5) {
        try {
          data.keyGenHmacMD5 = KeyGenerator.getInstance("HmacMD5");
        } catch (NoSuchAlgorithmException s) {
          logError(
              BaseMessages.getString(
                  PKG, "RandomValue.Log.HmacMD5AlgorithmException", s.getMessage()));
          return false;
        }
      }
      if (genHmacSHA1) {
        try {
          data.keyGenHmacSHA1 = KeyGenerator.getInstance("HmacSHA1");
        } catch (NoSuchAlgorithmException s) {
          logError(
              BaseMessages.getString(
                  PKG, "RandomValue.Log.HmacSHA1AlgorithmException", s.getMessage()));
          return false;
        }
      }
      if (uuid4) {
        data.u4 = new Uuid4Util();
      }
      return true;
    }
    return false;
  }
}
