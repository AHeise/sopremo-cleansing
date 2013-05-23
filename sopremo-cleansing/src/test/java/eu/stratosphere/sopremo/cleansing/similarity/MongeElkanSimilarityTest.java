package eu.stratosphere.sopremo.cleansing.similarity;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.similarity.set.MongeElkanSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class MongeElkanSimilarityTest extends EqualCloneTest<MongeElkanSimilarity> {
	@Override
	protected MongeElkanSimilarity createDefaultInstance(int index) {
		return new MongeElkanSimilarity(new PathSimilarity<TextNode>(new InputSelection(index),
			new JaroWinklerSimilarity(), new InputSelection(index + 1)));
	}

	@Test
	public void test() {
		ObjectNode left = JsonUtil.createObjectNode("names", new String[] { "Joe", "J.", "Joseph" });
		ObjectNode right = JsonUtil.createObjectNode("names2", new String[] { "Joseph" });
		MongeElkanSimilarity mongeElkanSimilarity = new MongeElkanSimilarity(new JaroWinklerSimilarity());

		Similarity<IJsonNode> pathSimilarity = new PathSimilarity<IArrayNode<IJsonNode>>(JsonUtil.createPath("names"),
			mongeElkanSimilarity, JsonUtil.createPath("names2"));
		SimilarityExpression expression = new SimilarityExpression(pathSimilarity);

		final double actualSim = ((INumericNode) expression.evaluate(JsonUtil.asArray(left, right))).getDoubleValue();
		Assert.assertEquals(0.8, actualSim, 0.1);
	}
}
