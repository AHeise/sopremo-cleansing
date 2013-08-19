package eu.stratosphere.sopremo.cleansing.scrubbing;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class IllegalCharacterRuleTest extends
		EqualCloneTest<IllegalCharacterRule> {

	private IllegalCharacterRule createRule(String ic,
			ValueCorrection correction) {
		IllegalCharacterRule rule = new IllegalCharacterRule(
				TextNode.valueOf(ic));
		rule.setValueCorrection(correction);
		return rule;
	}

	private IllegalCharacterRule createRule(String ic) {
		return this.createRule(ic, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected IllegalCharacterRule createDefaultInstance(int index) {
		return createRule(String.valueOf(index));
	}

	@Test
	public void shouldFindIllegalCharacters() {
		IllegalCharacterRule rule = createRule("fo");
		Assert.assertFalse(rule.validate(TextNode.valueOf("barfoo")));
	}

	@Test
	public void shouldNotFindIllegalCharacters() {
		IllegalCharacterRule rule = createRule("fo");
		Assert.assertTrue(rule.validate(TextNode.valueOf("barbar")));
	}

	@Test
	public void shouldFixViolatingValues() {
		IllegalCharacterRule rule = createRule("fo",
				CleansFunctions.REMOVE_ILLEGAL_CHARACTERS);
		TextNode node = TextNode.valueOf("foobar");
		Assert.assertEquals(TextNode.valueOf("bar"), rule.fix(node));
	}

	@Test
	public void shouldFixViolatingValuesMultipleTimes() {
		IllegalCharacterRule rule = createRule("fo",
				CleansFunctions.REMOVE_ILLEGAL_CHARACTERS);
		IArrayNode<IJsonNode> nodes = new ArrayNode<IJsonNode>(
				TextNode.valueOf("foobar"), TextNode.valueOf("barfoo"),
				TextNode.valueOf("fobaroo"));
		for (IJsonNode node : nodes)
			Assert.assertEquals(TextNode.valueOf("bar"), rule.fix(node));
	}

	@Test
	public void shouldRemoveRecordWithDefaultCorrection() {
		IllegalCharacterRule rule = createRule("fo");
		TextNode node = TextNode.valueOf("foobar");
		Assert.assertEquals(FilterRecord.Instance, rule.fix(node));
	}
}
