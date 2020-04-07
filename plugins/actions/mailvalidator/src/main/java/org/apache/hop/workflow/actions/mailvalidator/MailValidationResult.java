package org.apache.hop.workflow.actions.mailvalidator;

public class MailValidationResult {

  private boolean isvalide;

  private String errMsg;

  public MailValidationResult() {
    this.isvalide = false;
    this.errMsg = null;
  }

  public boolean isValide() {
    return this.isvalide;
  }

  public void setValide( boolean valid ) {
    this.isvalide = valid;
  }

  public String getErrorMessage() {
    return this.errMsg;
  }

  public void setErrorMessage( String errMsg ) {
    this.errMsg = errMsg;
  }

}
