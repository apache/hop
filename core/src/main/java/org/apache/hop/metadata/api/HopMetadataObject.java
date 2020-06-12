package org.apache.hop.metadata.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This class annotation signals to Hop Metadata that this object can be serialized.
 * It also provides information on how the object should be instantiated.
 */
@Retention( RetentionPolicy.RUNTIME )
public @interface HopMetadataObject {

  Class<? extends IHopMetadataObjectFactory> objectFactory() default HopMetadataDefaultObjectFactory.class;

}
