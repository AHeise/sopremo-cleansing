package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;

public class EntityMappingSerializationTest extends SopremoOperatorTestBase<EntityMapping> {

	@Override
	protected EntityMapping createDefaultInstance(int index) {
		EntityMapping entityMappingMock = new EntityMapping();
		return entityMappingMock;
	}
}
