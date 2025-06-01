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

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.util.Uuid4Util;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Get random value. */
public class RandomValue extends BaseTransform<RandomValueMeta, RandomValueData> {

  private static final Class<?> PKG = RandomValueMeta.class;

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
    Object[] row = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());

    int index = inputRowMeta.size();
    for (RandomValueMeta.RVField field : meta.getFields()) {
      switch (field.getType()) {
        case NUMBER:
          row[index] = data.randomGenerator.nextDouble();
          break;
        case INTEGER:
          row[index] = (long) data.randomGenerator.nextInt();
          break;
        case STRING:
          row[index] = Long.toString(Math.abs(data.randomGenerator.nextLong()), 32);
          break;
        case UUID:
          row[index] = UUID.randomUUID().toString();
          break;
        case UUID4:
          row[index] = data.u4.getUUID4AsString();
          break;
        case HMAC_MD5:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.RandomType.HMAC_MD5);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACMD5", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        case HMAC_SHA1:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.RandomType.HMAC_SHA1);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACSHA1", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        case HMAC_SHA256:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.RandomType.HMAC_SHA256);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACSHA1", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        case HMAC_SHA512:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.RandomType.HMAC_SHA512);
          } catch (Exception e) {
            logError(
                BaseMessages.getString(
                    PKG, "RandomValue.Log.ErrorGettingRandomHMACSHA1", e.getMessage()));
            setErrors(1);
            stopAll();
          }
          break;
        case HMAC_SHA384:
          try {
            row[index] = generateRandomMACHash(RandomValueMeta.RandomType.HMAC_SHA384);
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
      index++;
    }

    return row;
  }

  private String generateRandomMACHash(RandomValueMeta.RandomType algorithm) throws Exception {
    // Generates a secret key
    SecretKey sk = null;
    switch (algorithm) {
      case HMAC_MD5:
        sk = data.keyGenHmacMD5.generateKey();
        break;
      case HMAC_SHA1:
        sk = data.keyGenHmacSHA1.generateKey();
        break;
      case HMAC_SHA256:
        sk = data.keyGenHmacSHA256.generateKey();
        break;
      case HMAC_SHA384:
        sk = data.keyGenHmacSHA384.generateKey();
        break;
      case HMAC_SHA512:
        sk = data.keyGenHmacSHA512.generateKey();
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
    for (byte value : hashCode) {
      String b = Integer.toHexString(value);
      if (b.length() == 1) {
        b = "0" + b;
      }
      encoded.append(b.substring(b.length() - 2));
    }

    return encoded.toString();
  }

  @Override
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

    if (isRowLevel()) {
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

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms(getTransformMeta());

    if (!Utils.isEmpty(previous)) {
      data.readsRows = true;
    }
    boolean random = false;
    boolean genHmacMD5 = false;
    boolean genHmacSHA1 = false;
    boolean genHmacSHA256 = false;
    boolean genHmacSHA512 = false;
    boolean genHmacSHA384 = false;
    boolean uuid4 = false;

    for (RandomValueMeta.RVField field : meta.getFields()) {
      switch (field.getType()) {
        case NUMBER, INTEGER, STRING:
          random = true;
          break;
        case HMAC_MD5:
          genHmacMD5 = true;
          break;
        case HMAC_SHA1:
          genHmacSHA1 = true;
          break;
        case HMAC_SHA256:
          genHmacSHA256 = true;
          break;
        case HMAC_SHA512:
          genHmacSHA512 = true;
          break;
        case HMAC_SHA384:
          genHmacSHA384 = true;
          break;
        case UUID4:
          uuid4 = true;
          break;
        default:
          break;
      }
    }
    if (random) {
      if (StringUtils.isEmpty(meta.getSeed())) {
        data.randomGenerator = new Random();
      } else {
        long seed = Const.toLong(resolve(meta.getSeed()), 0);
        data.randomGenerator = new Random(seed);
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
    if (genHmacSHA256) {
      try {
        data.keyGenHmacSHA256 = KeyGenerator.getInstance("HmacSHA256");
      } catch (NoSuchAlgorithmException s) {
        logError(
            BaseMessages.getString(
                PKG, "RandomValue.Log.HmacSHA1AlgorithmException", s.getMessage()));
        return false;
      }
    }
    if (genHmacSHA512) {
      try {
        data.keyGenHmacSHA512 = KeyGenerator.getInstance("HmacSHA512");
      } catch (NoSuchAlgorithmException s) {
        logError(
            BaseMessages.getString(
                PKG, "RandomValue.Log.HmacSHA1AlgorithmException", s.getMessage()));
        return false;
      }
    }
    if (genHmacSHA384) {
      try {
        data.keyGenHmacSHA384 = KeyGenerator.getInstance("HmacSHA384");
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
}
