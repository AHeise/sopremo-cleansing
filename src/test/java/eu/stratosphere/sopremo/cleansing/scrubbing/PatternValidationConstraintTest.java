package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

public class PatternValidationConstraintTest extends
		EqualCloneTest<PatternValidationConstraint> {

	private PatternValidationConstraint createRule(String regex,
			ValueCorrection correction) {
		Pattern pattern = Pattern.compile(regex);
		PatternValidationConstraint rule = new PatternValidationConstraint(pattern);
		rule.setValueCorrection(correction);
		return rule;
	}

	private PatternValidationConstraint createRule(String regex) {
		return this.createRule(regex, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected PatternValidationConstraint createDefaultInstance(int index) {
		PatternValidationConstraint rule = this.createRule(String.valueOf(index),
			new DefaultValueCorrection(IntNode.valueOf(index)));
		return rule;
	}

	@Test
	public void shouldValidateCorrectValues() {
		PatternValidationConstraint rule = this.createRule("\\d{4}-\\d{4}");
		Assert.assertTrue(rule.validate(TextNode.valueOf("1000-2000")));
	}

	@Test
	public void shouldNotValidateWrongValues() {
		PatternValidationConstraint rule = this.createRule("\\d{4}-\\d{4}");
		Assert.assertFalse(rule.validate(TextNode.valueOf("1000_2000")));
	}

	@Test
	public void shouldRemoveWrongValues() {
		PatternValidationConstraint rule = this.createRule("\\d{4}-\\d{4}",
			ValidationRule.DEFAULT_CORRECTION);
		Assert.assertEquals(FilterRecord.Instance,
			rule.fix(TextNode.valueOf("1000_2000")));
	}
}
