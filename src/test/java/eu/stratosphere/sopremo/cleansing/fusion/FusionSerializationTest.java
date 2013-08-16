package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.Fusion;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;

public class FusionSerializationTest extends SopremoOperatorTestBase<Fusion> {

	@Override
	protected Fusion createDefaultInstance(int index) {
		// ObjectCreation foovar = new
		// ObjectCreation().addMapping("foobar"+index, new NonNullRule());
		// Scrubbing scrubbingMock = new Scrubbing();
		// scrubbingMock.setRuleExpression(foovar);
		// return scrubbingMock;

		ObjectCreation resolutions = new ObjectCreation().addMapping("index" + index, new MostFrequentResolution());
		Fusion fusionMock = new Fusion();
		fusionMock.setResolutionExpression(resolutions);
		return fusionMock;
	}
}
