package eu.stratosphere.sopremo.cleansing.scrubbing;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class IllegalCharacterRuleTest extends
		EqualCloneTest<IllegalCharacterRule> {

	@Override
	protected IllegalCharacterRule createDefaultInstance(int index) {
		return new IllegalCharacterRule(TextNode.valueOf(String.valueOf(index)));
	}

	@Test
	public void shouldFixViolatingValues() {
		IllegalCharacterRule rule = new IllegalCharacterRule(
				TextNode.valueOf("fo"));
		rule.setValueCorrection(CleansFunctions.REMOVE_ILLEGAL_CHARACTERS);
		TextNode node = TextNode.valueOf("foobar");
		Assert.assertEquals(TextNode.valueOf("bar"), rule.fix(node));
	}

	@Test
	public void shouldFixViolatingValuesMultipleTimes() {
		IllegalCharacterRule rule = new IllegalCharacterRule(
				TextNode.valueOf("fo"));
		rule.setValueCorrection(CleansFunctions.REMOVE_ILLEGAL_CHARACTERS);
		IArrayNode<IJsonNode> nodes = new ArrayNode<IJsonNode>(
				TextNode.valueOf("foobar"), TextNode.valueOf("barfoo"),
				TextNode.valueOf("fobaroo"));
		for (IJsonNode node : nodes)
			Assert.assertEquals(TextNode.valueOf("bar"), rule.fix(node));
	}
}
