package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

public class PatternValidationRuleTest extends
		EqualCloneTest<PatternValidationRule> {

	private PatternValidationRule createRule(String regex,
			ValueCorrection correction) {
		Pattern pattern = Pattern.compile(regex);
		PatternValidationRule rule = new PatternValidationRule(pattern);
		rule.setValueCorrection(correction);
		return rule;
	}

	private PatternValidationRule createRule(String regex) {
		return this.createRule(regex, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected PatternValidationRule createDefaultInstance(int index) {
		PatternValidationRule rule = this.createRule(String.valueOf(index),
				new DefaultValueCorrection(IntNode.valueOf(index)));
		return rule;
	}

	@Test
	public void shouldValidateCorrectValues() {
		PatternValidationRule rule = this.createRule("\\d{4}-\\d{4}");
		Assert.assertTrue(rule.validate(TextNode.valueOf("1000-2000")));
	}

	@Test
	public void shouldNotValidateWrongValues() {
		PatternValidationRule rule = this.createRule("\\d{4}-\\d{4}");
		Assert.assertFalse(rule.validate(TextNode.valueOf("1000_2000")));
	}

	@Test
	public void shouldRemoveWrongValues() {
		PatternValidationRule rule = this.createRule("\\d{4}-\\d{4}",
				ValidationRule.DEFAULT_CORRECTION);
		Assert.assertEquals(FilterRecord.Instance,
				rule.fix(TextNode.valueOf("1000_2000")));
	}
}
