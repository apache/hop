package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class MappingIODefinitionLoadSaveValidator implements IFieldLoadSaveValidator<MappingIODefinition> {
  final Random rand = new Random();
  @Override
  public MappingIODefinition getTestObject() {
    MappingIODefinition rtn = new MappingIODefinition();
    rtn.setDescription( UUID.randomUUID().toString() );
    rtn.setInputTransformName( UUID.randomUUID().toString() );
    rtn.setMainDataPath( rand.nextBoolean() );
    rtn.setOutputTransformName( UUID.randomUUID().toString() );
    rtn.setRenamingOnOutput( rand.nextBoolean() );
    List<MappingValueRename> renames = Arrays.asList(
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() ),
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() ),
        new MappingValueRename( UUID.randomUUID().toString(), UUID.randomUUID().toString() )
    );

    rtn.setValueRenames( renames );
    return rtn;
  }

  @Override
  public boolean validateTestObject( MappingIODefinition testObject, Object actual ) {
    if ( !( actual instanceof MappingIODefinition ) ) {
      return false;
    }
    MappingIODefinition actualInput = (MappingIODefinition) actual;
    return ( testObject.getXml().equals( actualInput.getXml() ) );
  }
}
