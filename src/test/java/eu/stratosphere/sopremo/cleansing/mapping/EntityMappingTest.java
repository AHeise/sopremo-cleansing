package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.type.JsonUtil;

public class EntityMappingTest extends SopremoOperatorTestBase<EntityMapping> {

	@Override
	protected EntityMapping createDefaultInstance(int index) {
		EntityMapping entityMapping = new EntityMapping();

		entityMapping.setInput(0, new Source("file:///0"));
		ObjectCreation mapping1 = new ObjectCreation();
		mapping1.addMapping("outputName", JsonUtil.createPath("0", "inputName"));

		final ArrayCreation assignments = new ArrayCreation(
			new NestedOperatorExpression(new Grouping().
				withGroupingKey(JsonUtil.createPath("0", "id")).
				withResultProjection(mapping1))
			);

		if (index > 0) {
			entityMapping.setInput(1, new Source("file:///1"));
			mapping1.addMapping("outputCity", JsonUtil.createPath("1", "inputCity"));
		}
		if (index > 1) {
			ObjectCreation mapping2 = new ObjectCreation();
			mapping1.addMapping("outputName2", JsonUtil.createPath("0", "inputName"));
			mapping1.addMapping("outputCity2", JsonUtil.createPath("1", "inputCity"));
			assignments.add(new NestedOperatorExpression(new Grouping().
				withGroupingKey(JsonUtil.createPath("1", "id")).
				withResultProjection(mapping2)));
			new Sink("file:///dummy").withInputs(entityMapping.getOutput(1));
		}

		entityMapping.setMappingExpression(assignments);
		return entityMapping;
	}
}
