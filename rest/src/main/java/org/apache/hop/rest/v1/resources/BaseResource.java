package org.apache.hop.rest.v1.resources;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.hop.core.Const;
import org.apache.hop.rest.Hop;

public abstract class BaseResource {
  protected final Hop hop = Hop.getInstance();

  protected Response getServerError(String errorMessage) {
    return getServerError(errorMessage, null, true);
  }

  protected Response getServerError(String errorMessage, boolean logOnServer) {
    return getServerError(errorMessage, null, logOnServer);
  }

  protected Response getServerError(String errorMessage, Exception e) {
    return getServerError(errorMessage, e, true);
  }

  protected Response getServerError(String errorMessage, Exception e, boolean logOnServer) {
    if (logOnServer) {
      if (e != null) {
        hop.getLog().logError(errorMessage, e);
      } else {
        hop.getLog().logError(errorMessage);
      }
    }
    return Response.serverError()
        .status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(errorMessage + (e == null ? "" : ("\n" + Const.getSimpleStackTrace(e))))
        .type(MediaType.TEXT_PLAIN)
        .build();
  }
}
