package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.cleansing.DataTransformation;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.type.JsonUtil;

public class DataTransformationTest extends SopremoOperatorTestBase<DataTransformation> {

	@Override
	protected DataTransformation createDefaultInstance(int index) {
		DataTransformation entityMapping = new DataTransformation();

		entityMapping.setInput(0, new Source("file:///0"));
		ObjectCreation mapping1 = new ObjectCreation().
			addMapping("outputName", JsonUtil.createPath("0", "inputName"));

		IdentifyOperator identifyOperator1 = new IdentifyOperator().
			withResultProjection(mapping1).
			withGroupingKey(JsonUtil.createPath("0", "id")).
			withInputs(entityMapping.getOutput(0));

		final ArrayCreation assignments = new ArrayCreation(new NestedOperatorExpression(identifyOperator1));

		if (index > 0) {
			entityMapping.setInput(1, new Source("file:///1"));
			mapping1.addMapping("outputCity", JsonUtil.createPath("1", "inputCity"));
		}
		if (index > 1) {
			ObjectCreation mapping2 = new ObjectCreation();
			mapping2.addMapping("outputName2", JsonUtil.createPath("0", "inputName"));
			mapping2.addMapping("outputCity2", JsonUtil.createPath("1", "inputCity"));

			IdentifyOperator identifyOperator2 = new IdentifyOperator().
				withResultProjection(mapping2).
				withGroupingKey(JsonUtil.createPath("1", "id")).
				withInputs(entityMapping.getOutput(0));

			assignments.add(new NestedOperatorExpression(identifyOperator2));
			new Sink("file:///dummy").withInputs(entityMapping.getOutput(1));
		}

		entityMapping.setMappingExpression(assignments);
		return entityMapping;
	}
}
