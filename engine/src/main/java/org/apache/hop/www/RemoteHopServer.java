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
 */

package org.apache.hop.www;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.TrustSelfSignedStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.server.ServerConnectionManager;

/**
 * Client used to interact with a remote Hop server over HTTP(S). It wraps a {@link HopServerMeta}
 * that holds the connection metadata and exposes the operations that used to live on {@code
 * HopServerMeta} itself.
 */
public class RemoteHopServer {
  // Resolve messages against the HopServerMeta bundle (org.apache.hop.server), which holds the
  // HopServer.* keys used by the operations that were moved here from HopServerMeta.
  private static final Class<?> PKG = HopServerMeta.class;

  /** A variable to configure the number of retries for Hop server to send data */
  @Variable(
      value = "0",
      description = "A variable to configure the number of retries for Hop server to send data")
  public static final String HOP_SERVER_RETRIES = "HOP_SERVER_RETRIES";

  /** to configure the time in milliseconds to wait before retrying to send data */
  @Variable(
      value = "1000",
      description =
          "A variable to configure the time in milliseconds to wait before retrying to send data")
  public static final String HOP_SERVER_RETRY_BACKOFF_INCREMENTS =
      "HOP_SERVER_RETRY_BACKOFF_INCREMENTS";

  public static final String PROTOCOL_HTTP = "http";
  public static final String PROTOCOL_HTTPS = "https";
  private static final Random RANDOM = new Random();
  private static final String STRING_HOP_SERVER = "Hop Server";

  private static final String CONST_NAME = "/?name=";
  private static final String CONST_XML = "&xml=Y";
  private static final String CONST_ID = "&id=";

  private final ILogChannel log;
  private final HopServerMeta serverMeta;

  private static int getNumberOfHopServerRetries(IVariables variables) {
    try {
      return Integer.parseInt(variables.getVariable(HOP_SERVER_RETRIES, "0"));
    } catch (Exception e) {
      return 0;
    }
  }

  public static int getBackoffIncrements(IVariables variables) {
    try {
      return Integer.parseInt(variables.getVariable(HOP_SERVER_RETRY_BACKOFF_INCREMENTS, "1000"));
    } catch (Exception e) {
      return 1000;
    }
  }

  public RemoteHopServer(HopServerMeta serverMeta) {
    this.serverMeta = requireNonNull(serverMeta, "serverMeta");
    this.log = new LogChannel(STRING_HOP_SERVER);
  }

  public ILogChannel getLog() {
    return log;
  }

  public HopServerMeta getServerMeta() {
    return serverMeta;
  }

  public String getName() {
    return serverMeta.getName();
  }

  public String getPortSpecification(IVariables variables) {
    String realPort = variables.resolve(serverMeta.getPort());
    String portSpec = ":" + realPort;
    if (Utils.isEmpty(realPort) || realPort.equals("80")) {
      portSpec = "";
    }
    return portSpec;
  }

  public String constructUrl(IVariables variables, String serviceAndArguments) {
    String realHostname = variables.resolve(serverMeta.getHostname());
    String proxyHostname = variables.resolve(serverMeta.getProxyHostname());
    if (!Utils.isEmpty(proxyHostname) && realHostname.equals("localhost")) {
      realHostname = "127.0.0.1";
    }

    if (!StringUtils.isBlank(serverMeta.getWebAppName())) {
      serviceAndArguments =
          "/" + variables.resolve(serverMeta.getWebAppName()) + serviceAndArguments;
    }

    String result =
        (serverMeta.isSslMode() ? PROTOCOL_HTTPS : PROTOCOL_HTTP)
            + "://"
            + realHostname
            + getPortSpecification(variables)
            + serviceAndArguments;
    result = Const.replace(result, " ", "%20");
    return result;
  }

  HttpPost buildSendXmlMethod(IVariables variables, byte[] content, String service) {
    String encoding = Const.UTF_8;
    return buildSendMethod(variables, content, encoding, service, "text/xml");
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpPost buildSendMethod(
      IVariables variables, byte[] content, String encoding, String service, String contentType) {
    // Prepare HTTP put
    //
    String urlString = constructUrl(variables, service);
    if (log.isDebug()) {
      log.logDebug(BaseMessages.getString(PKG, "HopServer.DEBUG_ConnectingTo", urlString));
    }
    HttpPost postMethod = new HttpPost(urlString);

    // Request content will be retrieved directly from the input stream
    //
    HttpEntity entity = new ByteArrayEntity(content, ContentType.APPLICATION_OCTET_STREAM);

    postMethod.setEntity(entity);
    postMethod.addHeader(new BasicHeader("Accept", contentType + ";charset=" + encoding));

    return postMethod;
  }

  public String sendXml(IVariables variables, String xml, String service) throws Exception {
    String encoding = getXmlEncoding(xml);
    HttpPost method =
        buildSendMethod(variables, xml.getBytes(encoding), encoding, service, "text/xml");
    try {
      return executeAuth(variables, method);
    } finally {
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG,
                "HopServer.DETAILED_SentXmlToService",
                service,
                variables.resolve(serverMeta.getHostname())));
      }
    }
  }

  public String sendJson(IVariables variables, String json, String service) throws Exception {
    String encoding = Const.UTF_8;
    HttpPost method =
        buildSendMethod(variables, json.getBytes(encoding), encoding, service, "application/json");
    try {
      return executeAuth(variables, method);
    } finally {
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG,
                "HopServer.DETAILED_SentXmlToService",
                service,
                variables.resolve(serverMeta.getHostname())));
      }
    }
  }

  private String getXmlEncoding(String xml) {
    Pattern xmlHeadPattern = Pattern.compile("<\\?xml.* encoding=\"(.*)\"");
    Matcher matcher = xmlHeadPattern.matcher(xml);
    if (matcher.find()) {
      return matcher.group();
    }

    return Const.UTF_8;
  }

  /** Throws if not ok */
  private void handleStatus(
      IVariables variables, ClassicHttpRequest method, ClassicHttpResponse response, int status)
      throws HopException {
    if (status >= 300) {
      String message;
      if (status == HttpStatus.SC_NOT_FOUND) {
        message =
            String.format(
                "%s%s%s%s",
                BaseMessages.getString(PKG, "HopServer.Error.404.Title"),
                Const.CR,
                Const.CR,
                BaseMessages.getString(PKG, "HopServer.Error.404.Message"));
      } else {
        message =
            String.format(
                "HTTP Status %d - %s - %s",
                status, method.getRequestUri(), response.getReasonPhrase());
      }
      throw new HopException(message);
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpPost buildSendExportMethod(IVariables variables, String type, String load, InputStream is) {
    String serviceUrl = RegisterPackageServlet.CONTEXT_PATH;
    if (type != null && load != null) {
      serviceUrl +=
          "/?"
              + RegisterPackageServlet.PARAMETER_TYPE
              + "="
              + type
              + "&"
              + RegisterPackageServlet.PARAMETER_LOAD
              + "="
              + URLEncoder.encode(load, UTF_8);
    }

    String urlString = constructUrl(variables, serviceUrl);
    if (log.isDebug()) {
      log.logDebug(BaseMessages.getString(PKG, "HopServer.DEBUG_ConnectingTo", urlString));
    }

    HttpPost method = new HttpPost(urlString);
    method.setEntity(new InputStreamEntity(is, ContentType.APPLICATION_OCTET_STREAM));
    method.addHeader(new BasicHeader("Content-Type", "binary/zip"));

    return method;
  }

  /**
   * Send an exported archive over to this hop server
   *
   * @param filename The archive to send
   * @param type The type of file to add to the hop server (AddExportServlet.TYPE_*)
   * @param load The filename to load in the archive (the .hwf or .hpl)
   * @return the XML of the web result
   * @throws Exception in case something goes awry
   */
  public String sendExport(IVariables variables, String filename, String type, String load)
      throws Exception {
    // Request content will be retrieved directly from the input stream
    try (InputStream is = HopVfs.getInputStream(HopVfs.getFileObject(filename))) {
      // Execute request
      HttpPost method = buildSendExportMethod(variables, type, load, is);
      try {
        return executeAuth(variables, method);
      } finally {
        if (log.isDetailed()) {
          log.logDetailed(
              BaseMessages.getString(
                  PKG,
                  "HopServer.DETAILED_SentExportToService",
                  RegisterPackageServlet.CONTEXT_PATH,
                  variables.resolve(serverMeta.getHostname())));
        }
      }
    }
  }

  /**
   * Executes method with authentication.
   *
   * @param method the request to execute
   * @return the response body
   * @throws IOException on transport failure
   * @throws HopException if response not ok
   */
  private String executeAuth(IVariables variables, ClassicHttpRequest method)
      throws IOException, HopException {
    ClassicHttpResponse httpResponse =
        (ClassicHttpResponse) getHttpClient().execute(method, getAuthContext(variables));
    return getResponse(variables, method, httpResponse);
  }

  private String getResponse(
      IVariables variables, ClassicHttpRequest method, ClassicHttpResponse httpResponse)
      throws IOException, HopException {
    int statusCode = httpResponse.getCode();
    // The status code
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG, "HopServer.DEBUG_ResponseStatus", Integer.toString(statusCode)));
    }

    String responseBody = getResponseBodyAsString(httpResponse.getEntity().getContent());
    if (log.isDebug()) {
      log.logDebug(BaseMessages.getString(PKG, "HopServer.DEBUG_ResponseBody", responseBody));
    }

    // throw if not ok
    handleStatus(variables, method, httpResponse, statusCode);

    return responseBody;
  }

  private void addCredentials(IVariables variables, HttpClientContext context) {

    String host = variables.resolve(serverMeta.getHostname());
    int port = Const.toInt(variables.resolve(serverMeta.getPort()), 80);
    String userName = variables.resolve(serverMeta.getUsername());
    String password =
        Encr.decryptPasswordOptionallyEncrypted(variables.resolve(serverMeta.getPassword()));
    String proxyHost = variables.resolve(serverMeta.getProxyHostname());

    BasicCredentialsProvider provider = new BasicCredentialsProvider();
    char[] passwordChars = password != null ? password.toCharArray() : new char[0];
    UsernamePasswordCredentials credentials =
        new UsernamePasswordCredentials(userName, passwordChars);
    if (!Utils.isEmpty(proxyHost) && host.equals("localhost")) {
      host = "127.0.0.1";
    }
    provider.setCredentials(new AuthScope(host, port), credentials);
    context.setCredentialsProvider(provider);
    // Generate BASIC scheme object and add it to the local auth cache
    HttpHost target =
        new HttpHost(serverMeta.isSslMode() ? PROTOCOL_HTTPS : PROTOCOL_HTTP, host, port);
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    basicAuth.initPreemptive(credentials);
    authCache.put(target, basicAuth);
    context.setAuthCache(authCache);
  }

  private void addProxy(IVariables variables, HttpClientContext context) {
    String proxyHost = variables.resolve(serverMeta.getProxyHostname());
    String proxyPort = variables.resolve(serverMeta.getProxyPort());
    String nonProxyHosts = variables.resolve(serverMeta.getNonProxyHosts());

    String hostName = variables.resolve(serverMeta.getHostname());
    if (Utils.isEmpty(proxyHost) || Utils.isEmpty(proxyPort)) {
      return;
    }
    // skip applying proxy if non-proxy host matches
    if (!Utils.isEmpty(nonProxyHosts) && hostName.matches(nonProxyHosts)) {
      return;
    }
    HttpHost httpHost = new HttpHost(PROTOCOL_HTTP, proxyHost, Integer.parseInt(proxyPort));

    RequestConfig requestConfig = RequestConfig.custom().setProxy(httpHost).build();

    context.setRequestConfig(requestConfig);
  }

  /**
   * @return HttpClientContext with authorization credentials
   */
  protected HttpClientContext getAuthContext(IVariables variables) {
    HttpClientContext context = HttpClientContext.create();
    addCredentials(variables, context);
    addProxy(variables, context);
    return context;
  }

  public String execService(IVariables variables, String service, boolean retry) throws Exception {
    int tries = 0;
    int maxRetries = 0;
    int retryBackoffIncrements = getBackoffIncrements(variables);
    if (retry) {
      maxRetries = getNumberOfHopServerRetries(variables);
    }
    while (true) {
      try {
        return execService(variables, service);
      } catch (Exception e) {
        if (tries >= maxRetries) {
          throw e;
        } else {
          try {
            Thread.sleep(getDelay(tries, retryBackoffIncrements));
          } catch (InterruptedException e2) {
            // ignore
          }
        }
      }
      tries++;
    }
  }

  public static long getDelay(int trial, int retryBackoffIncrements) {
    long current = retryBackoffIncrements;
    long previous = 0;
    for (int i = 0; i < trial; i++) {
      long tmp = current;
      current = current + previous;
      previous = tmp;
    }
    return current + RANDOM.nextInt((int) Math.min(Integer.MAX_VALUE, current / 4L));
  }

  public String execService(IVariables variables, String service) throws Exception {
    return execService(variables, service, new HashMap<>());
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  String getResponseBodyAsString(InputStream is) throws IOException {
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, UTF_8));
    StringBuilder bodyBuffer = new StringBuilder();
    String line;

    try {
      while ((line = bufferedReader.readLine()) != null) {
        bodyBuffer.append(line);
      }
    } finally {
      bufferedReader.close();
    }

    return bodyBuffer.toString();
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpGet buildExecuteServiceMethod(
      IVariables variables, String service, Map<String, String> headerValues) {
    HttpGet method = new HttpGet(constructUrl(variables, service));

    for (String key : headerValues.keySet()) {
      method.setHeader(key, headerValues.get(key));
    }
    return method;
  }

  public String execService(IVariables variables, String service, Map<String, String> headerValues)
      throws Exception {
    // Prepare HTTP get
    HttpGet method = buildExecuteServiceMethod(variables, service, headerValues);
    // Execute request
    try {
      ClassicHttpResponse httpResponse =
          (ClassicHttpResponse) getHttpClient().execute(method, getAuthContext(variables));
      int statusCode = httpResponse.getCode();

      // The status code
      if (log.isDebug()) {
        log.logDebug(
            BaseMessages.getString(
                PKG, "HopServer.DEBUG_ResponseStatus", Integer.toString(statusCode)));
      }

      String responseBody = getResponseBodyAsString(httpResponse.getEntity().getContent());

      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG,
                "HopServer.DETAILED_FinishedReading",
                Integer.toString(responseBody.getBytes().length)));
      }
      if (log.isDebug()) {
        log.logDebug(BaseMessages.getString(PKG, "HopServer.DEBUG_ResponseBody", responseBody));
      }

      if (statusCode >= 400) {
        throw new HopException(
            String.format(
                "HTTP Status %d - %s - %s",
                statusCode, method.getRequestUri(), httpResponse.getReasonPhrase()));
      }

      return responseBody;
    } finally {
      if (log.isDetailed()) {
        log.logDetailed(
            BaseMessages.getString(
                PKG,
                "HopServer.DETAILED_ExecutedService",
                service,
                variables.resolve(serverMeta.getHostname())));
      }
    }
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  HttpClient getHttpClient() throws HopException {
    try {
      if (serverMeta.isSslMode()) {
        // Connect over an HTTPS connection
        //
        TrustStrategy acceptingTrustStrategy = new TrustSelfSignedStrategy();
        SSLContext sslContext =
            SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();

        SSLConnectionSocketFactory socketFactory =
            new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        return HttpClients.custom().setConnectionManagerShared(true).build();
      } else {
        // Connect using a regular HTTP connection, use connection manager to limit the number of
        // open connections to hop servers.
        //
        return ServerConnectionManager.getInstance().createHttpClient();
      }
    } catch (Exception e) {
      throw new HopException("Error creating new HTTP client", e);
    }
  }

  public HopServerStatus requestServerStatus(IVariables variables) throws Exception {
    String xml = execService(variables, GetStatusServlet.CONTEXT_PATH + "/?xml=Y");
    return HopServerStatus.fromXml(xml);
  }

  public HopServerPipelineStatus requestPipelineStatus(
      IVariables variables, String pipelineName, String serverObjectId, int startLogLineNr)
      throws Exception {
    return requestPipelineStatus(variables, pipelineName, serverObjectId, startLogLineNr, false);
  }

  public HopServerPipelineStatus requestPipelineStatus(
      IVariables variables,
      String pipelineName,
      String serverObjectId,
      int startLogLineNr,
      boolean sendResultXmlWithStatus)
      throws Exception {
    String query =
        GetPipelineStatusServlet.CONTEXT_PATH
            + CONST_NAME
            + URLEncoder.encode(pipelineName, UTF_8)
            + CONST_ID
            + Const.NVL(serverObjectId, "")
            + "&xml=Y&from="
            + startLogLineNr;
    if (sendResultXmlWithStatus) {
      query = query + "&" + GetPipelineStatusServlet.SEND_RESULT + "=Y";
    }
    String xml = execService(variables, query, true);
    return HopServerPipelineStatus.fromXml(xml);
  }

  public HopServerWorkflowStatus requestWorkflowStatus(
      IVariables variables, String workflowName, String serverObjectId, int startLogLineNr)
      throws Exception {
    String xml =
        execService(
            variables,
            GetWorkflowStatusServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(workflowName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + "&xml=Y&from="
                + startLogLineNr,
            true);
    return HopServerWorkflowStatus.fromXml(xml);
  }

  public WebResult requestStopPipeline(
      IVariables variables, String pipelineName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            StopPipelineServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(pipelineName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + CONST_XML);
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestPauseResumePipeline(
      IVariables variables, String pipelineName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            PausePipelineServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(pipelineName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + CONST_XML);
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestRemovePipeline(
      IVariables variables, String pipelineName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            RemovePipelineServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(pipelineName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + CONST_XML);
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestRemoveWorkflow(
      IVariables variables, String workflowName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            RemoveWorkflowServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(workflowName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + CONST_XML);
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestStopWorkflow(
      IVariables variables, String pipelineName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            StopWorkflowServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(pipelineName, UTF_8)
                + "&xml=Y&id="
                + Const.NVL(serverObjectId, ""));
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestStartPipeline(
      IVariables variables, String pipelineName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            StartPipelineServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(pipelineName, UTF_8)
                + CONST_ID
                + Const.NVL(serverObjectId, "")
                + CONST_XML);
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestStartWorkflow(
      IVariables variables, String workflowName, String serverObjectId) throws Exception {
    String xml =
        execService(
            variables,
            StartWorkflowServlet.CONTEXT_PATH
                + CONST_NAME
                + URLEncoder.encode(workflowName, UTF_8)
                + "&xml=Y&id="
                + Const.NVL(serverObjectId, ""));
    return WebResult.fromXmlString(xml);
  }

  public WebResult requestShutdownServer(IVariables variables) throws Exception {
    execService(variables, ShutdownServlet.CONTEXT_PATH);
    return WebResult.OK;
  }

  /**
   * Sniff rows on a the hop server, return xml containing the row metadata and data.
   *
   * @param pipelineName pipeline name
   * @param id the id on the server
   * @param transformName transform name
   * @param copyNr transform copy number
   * @param lines lines number
   * @param type transform type
   * @return xml with row metadata and data
   * @throws Exception in case something goes awry
   */
  public String sniffTransform(
      IVariables variables,
      String pipelineName,
      String transformName,
      String id,
      String copyNr,
      int lines,
      String type)
      throws Exception {
    return execService(
        variables,
        SniffTransformServlet.CONTEXT_PATH
            + "/?pipeline="
            + URLEncoder.encode(pipelineName, UTF_8)
            + CONST_ID
            + URLEncoder.encode(id, UTF_8)
            + "&transform="
            + URLEncoder.encode(transformName, UTF_8)
            + "&copynr="
            + copyNr
            + "&type="
            + type
            + "&lines="
            + lines
            + CONST_XML);
  }

  /**
   * Monitors a remote pipeline every 5 seconds.
   *
   * @param log the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param pipelineName the pipeline name
   */
  public void monitorRemotePipeline(
      IVariables variables, ILogChannel log, String serverObjectId, String pipelineName) {
    monitorRemotePipeline(variables, log, serverObjectId, pipelineName, 5);
  }

  /**
   * Monitors a remote pipeline at the specified interval.
   *
   * @param log the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param pipelineName the pipeline name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemotePipeline(
      IVariables variables,
      ILogChannel log,
      String serverObjectId,
      String pipelineName,
      int sleepTimeSeconds) {
    long errors = 0;
    boolean allFinished = false;
    while (!allFinished && errors == 0) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if (allFinished && errors == 0) {
        try {
          HopServerPipelineStatus pipelineStatus =
              requestPipelineStatus(variables, pipelineName, serverObjectId, 0);
          if (pipelineStatus.isRunning()) {
            if (log.isDetailed()) {
              log.logDetailed(pipelineName, "Remote pipeline is still running.");
            }
            allFinished = false;
          } else {
            if (log.isDetailed()) {
              log.logDetailed(pipelineName, "Remote pipeline has finished.");
            }
          }
          Result result = pipelineStatus.getResult();
          errors += result.getNrErrors();
        } catch (Exception e) {
          errors += 1;
          log.logError(
              pipelineName,
              "Unable to contact remote hop server '"
                  + serverMeta.getName()
                  + "' to check pipeline status : "
                  + e);
        }
      }

      //
      // Keep waiting until all pipelines have finished
      // If needed, we stop them again and again until they yield.
      //
      if (!allFinished) {
        // Not finished or error: wait a bit longer
        if (log.isDetailed()) {
          log.logDetailed(
              pipelineName, "The remote pipeline is still running, waiting a few seconds...");
        }
        try {
          Thread.sleep(sleepTimeSeconds * 1000L);
        } catch (Exception e) {
          // Ignore errors
        }
      }
    }

    log.logBasic(pipelineName, "The remote pipeline has finished.");
  }

  /**
   * Monitors a remote workflow every 5 seconds.
   *
   * @param log the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param workflowName the workflow name
   */
  public void monitorRemoteWorkflow(
      IVariables variables, ILogChannel log, String serverObjectId, String workflowName) {
    monitorRemoteWorkflow(variables, log, serverObjectId, workflowName, 5);
  }

  /**
   * Monitors a remote workflow at the specified interval.
   *
   * @param log the log channel interface
   * @param serverObjectId the HopServer object ID
   * @param workflowName the workflow name
   * @param sleepTimeSeconds the sleep time (in seconds)
   */
  public void monitorRemoteWorkflow(
      IVariables variables,
      ILogChannel log,
      String serverObjectId,
      String workflowName,
      int sleepTimeSeconds) {
    long errors = 0;
    boolean allFinished = false;
    while (!allFinished && errors == 0) {
      allFinished = true;
      errors = 0L;

      // Check the remote server
      if (allFinished && errors == 0) {
        try {
          HopServerWorkflowStatus workflowStatus =
              requestWorkflowStatus(variables, workflowName, serverObjectId, 0);
          if (workflowStatus.isRunning()) {
            if (log.isDetailed()) {
              log.logDetailed(workflowName, "Remote workflow is still running.");
            }
            allFinished = false;
          } else {
            if (log.isDetailed()) {
              log.logDetailed(workflowName, "Remote workflow has finished.");
            }
          }
          Result result = workflowStatus.getResult();
          errors += result.getNrErrors();
        } catch (Exception e) {
          errors += 1;
          log.logError(
              workflowName,
              "Unable to contact remote hop server '"
                  + serverMeta.getName()
                  + "' to check workflow status : "
                  + e);
        }
      }

      //
      // Keep waiting until all pipelines have finished
      // If needed, we stop them again and again until they yield.
      //
      if (!allFinished) {
        // Not finished or error: wait a bit longer
        if (log.isDetailed()) {
          log.logDetailed(
              workflowName, "The remote workflow is still running, waiting a few seconds...");
        }
        try {
          Thread.sleep(sleepTimeSeconds * 1000L);
        } catch (Exception e) {
          // Ignore errors
        }
      }
    }

    log.logBasic(workflowName, "The remote workflow has finished.");
  }
}
