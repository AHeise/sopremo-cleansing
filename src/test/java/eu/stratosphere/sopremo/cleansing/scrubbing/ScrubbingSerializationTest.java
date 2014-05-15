package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.Scrubbing;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;

public class ScrubbingSerializationTest extends SopremoOperatorTestBase<Scrubbing> {

	@Override
	protected Scrubbing createDefaultInstance(int index) {
		ObjectCreation foovar = new ObjectCreation().addMapping("foobar" + index, new NonNullConstraint());
		Scrubbing scrubbingMock = new Scrubbing();
		scrubbingMock.setRuleExpression(foovar);
		return scrubbingMock;
	}
}
