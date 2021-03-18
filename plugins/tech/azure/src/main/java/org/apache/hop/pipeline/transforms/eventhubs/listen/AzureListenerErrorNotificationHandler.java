package org.apache.hop.pipeline.transforms.eventhubs.listen;

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

import java.util.function.Consumer;

// The general notification handler is an object that derives from Consumer<> and takes an ExceptionReceivedEventArgs object
// as an argument. The argument provides the details of the error: the exception that occurred and the action (what EventProcessorHost
// was doing) during which the error occurred. The complete list of actions can be found in EventProcessorHostActionStrings.\
//
public class AzureListenerErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs>{

  private AzureListener azureTransform;

  public AzureListenerErrorNotificationHandler( AzureListener azureTransform ) {
    this.azureTransform = azureTransform;
  }

  @Override
  public void accept(ExceptionReceivedEventArgs t) {

    azureTransform.logError( "Host " + t.getHostname() + " received general error notification during " + t.getAction() + ": " + t.getException().toString());
    azureTransform.setErrors( 1 );
    azureTransform.stopAll();

  }
}