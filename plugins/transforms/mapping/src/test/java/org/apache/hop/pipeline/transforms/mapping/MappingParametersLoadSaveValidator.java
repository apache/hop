package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;

import java.util.Random;
import java.util.UUID;

public class MappingParametersLoadSaveValidator implements IFieldLoadSaveValidator<MappingParameters> {
  final Random rand = new Random();
  @Override
  public MappingParameters getTestObject() {
    MappingParameters rtn = new MappingParameters();
    rtn.setVariable( new String[] {
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()
    } );
    rtn.setInputField( new String[] {
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()
    } );
    rtn.setInheritingAllVariables( rand.nextBoolean() );
    return rtn;
  }

  @Override
  public boolean validateTestObject( MappingParameters testObject, Object actual ) {
    if ( !( actual instanceof MappingParameters ) ) {
      return false;
    }
    MappingParameters actualInput = (MappingParameters) actual;
    return ( testObject.getXml().equals( actualInput.getXml() ) );
  }
}