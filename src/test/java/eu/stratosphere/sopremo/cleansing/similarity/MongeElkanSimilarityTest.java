package eu.stratosphere.sopremo.cleansing.similarity;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.similarity.set.MongeElkanSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.ObjectNode;

public class MongeElkanSimilarityTest extends EqualCloneTest<MongeElkanSimilarity> {
	@Override
	protected MongeElkanSimilarity createDefaultInstance(int index) {
		return new MongeElkanSimilarity(new JaroWinklerSimilarity());
	}

	@Test
	public void test() {
		ObjectNode left = JsonUtil.createObjectNode("names", new String[] { "Joe", "J.", "Joseph" });
		ObjectNode right = JsonUtil.createObjectNode("names2", new String[] { "Joseph" });
		MongeElkanSimilarity mongeElkanSimilarity = new MongeElkanSimilarity(new JaroWinklerSimilarity());

		SimilarityExpression expression = new SimilarityExpression(JsonUtil.createPath("0", "names"),
				mongeElkanSimilarity, JsonUtil.createPath("1", "names2"));

		final double actualSim = ((INumericNode) expression.evaluate(JsonUtil.asArray(left, right))).getDoubleValue();
		Assert.assertEquals(0.8, actualSim, 0.1);
	}
}
