/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.gs;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.StorageRetryStrategy;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;

/**
 * Logs every retry the Google Cloud Storage client makes. The client retries timeouts and temporary
 * errors on its own and says nothing about it, so a slow or flaky connection looks exactly like a
 * hang. This decorates the configured strategy: the decision whether to retry is left to it
 * unchanged, and each time it decides to retry, a line is logged at the basic level naming the
 * error and, when known, the {@code gs://} object involved.
 *
 * <p>The client asks for a decision before it checks its attempt budget, so the last line before
 * giving up also says it is trying again; the error that follows says otherwise.
 */
class LoggingStorageRetryStrategy implements StorageRetryStrategy {

  private static final long serialVersionUID = 1L;

  private static final Class<?> PKG = LoggingStorageRetryStrategy.class;

  /** Where a retry is reported. Serializable because the storage options that hold it are. */
  @FunctionalInterface
  interface RetryLog extends Serializable {
    void retrying(String message);
  }

  private final Reporter reporter;
  private final ResultRetryAlgorithm<?> idempotentHandler;
  private final ResultRetryAlgorithm<?> nonIdempotentHandler;

  LoggingStorageRetryStrategy(StorageRetryStrategy delegate, GoogleCloudConfig config) {
    this(
        delegate,
        config,
        message -> LogChannel.GENERAL.logBasic("Google Cloud Storage: " + message));
  }

  LoggingStorageRetryStrategy(
      StorageRetryStrategy delegate, GoogleCloudConfig config, RetryLog retryLog) {
    this.reporter =
        new Reporter(
            retryLog,
            Const.toInt(config.getConnectionTimeout(), 20),
            Const.toInt(config.getReadTimeout(), 20));
    this.idempotentHandler = reporting(delegate.getIdempotentHandler());
    this.nonIdempotentHandler = reporting(delegate.getNonidempotentHandler());
  }

  @Override
  public ResultRetryAlgorithm<?> getIdempotentHandler() {
    return idempotentHandler;
  }

  @Override
  public ResultRetryAlgorithm<?> getNonidempotentHandler() {
    return nonIdempotentHandler;
  }

  private <T> ResultRetryAlgorithm<T> reporting(ResultRetryAlgorithm<T> algorithm) {
    return new Reporting<>(algorithm, reporter);
  }

  /** Answers exactly what the wrapped algorithm answers, and reports it when that is a retry. */
  private static final class Reporting<T> implements ResultRetryAlgorithm<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final ResultRetryAlgorithm<T> algorithm;
    private final Reporter reporter;

    private Reporting(ResultRetryAlgorithm<T> algorithm, Reporter reporter) {
      this.algorithm = algorithm;
      this.reporter = reporter;
    }

    @Override
    public TimedAttemptSettings createNextAttempt(
        Throwable previousThrowable, T previousResponse, TimedAttemptSettings previousSettings) {
      return algorithm.createNextAttempt(previousThrowable, previousResponse, previousSettings);
    }

    @Override
    public boolean shouldRetry(Throwable previousThrowable, T previousResponse) {
      boolean retry = algorithm.shouldRetry(previousThrowable, previousResponse);
      if (retry && previousThrowable != null) {
        reporter.report(previousThrowable);
      }
      return retry;
    }
  }

  /** Turns a retried error into a line in the log. */
  private static final class Reporter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RetryLog retryLog;
    private final int connectTimeout;
    private final int readTimeout;

    private Reporter(RetryLog retryLog, int connectTimeout, int readTimeout) {
      this.retryLog = retryLog;
      this.connectTimeout = connectTimeout;
      this.readTimeout = readTimeout;
    }

    void report(Throwable error) {
      try {
        retryLog.retrying(message(error, GoogleStorageObjectContext.current()));
      } catch (RuntimeException e) {
        // This runs inside the client's retry decision: failing to log must never change it.
      }
    }

    private String message(Throwable error, String object) {
      SocketTimeoutException timeout = findTimeout(error);
      if (timeout != null) {
        String what = Const.NVL(timeout.getMessage(), "timed out");
        return object == null
            ? BaseMessages.getString(
                PKG, "LoggingStorageRetryStrategy.Timeout", what, connectTimeout, readTimeout)
            : BaseMessages.getString(
                PKG,
                "LoggingStorageRetryStrategy.Timeout.Object",
                object,
                what,
                connectTimeout,
                readTimeout);
      }
      String description = describe(error);
      return object == null
          ? BaseMessages.getString(PKG, "LoggingStorageRetryStrategy.TemporaryError", description)
          : BaseMessages.getString(
              PKG, "LoggingStorageRetryStrategy.TemporaryError.Object", object, description);
    }
  }

  private static SocketTimeoutException findTimeout(Throwable throwable) {
    for (Throwable t = throwable; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof SocketTimeoutException) {
        return (SocketTimeoutException) t;
      }
    }
    return null;
  }

  /**
   * A one-line description of a temporary error. An HTTP status is reported with the message GCS
   * sent along; anything else - a reset connection, say - by its root cause, since that is where
   * the client keeps the useful part.
   */
  static String describe(Throwable throwable) {
    if (throwable instanceof BaseServiceException) {
      BaseServiceException serviceException = (BaseServiceException) throwable;
      int code = serviceException.getCode();
      if (code > 0) {
        // The message often starts with the status already, as in "503 Service Unavailable".
        String message = Const.NVL(serviceException.getMessage(), "");
        return message.startsWith(Integer.toString(code))
            ? "HTTP " + message
            : "HTTP " + code + " " + message;
      }
    }
    Throwable root = throwable;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String name = root.getClass().getSimpleName();
    return root.getMessage() == null ? name : name + ": " + root.getMessage();
  }
}
