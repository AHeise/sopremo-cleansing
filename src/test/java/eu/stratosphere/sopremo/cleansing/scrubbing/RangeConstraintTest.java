package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IntNode;

public class RangeConstraintTest extends EqualCloneTest<RangeConstraint> {

	private RangeConstraint createRule(int min, int max, ValueCorrection correction) {
		RangeConstraint rule = new RangeConstraint(IntNode.valueOf(min),
				IntNode.valueOf(max));
		rule.setValueCorrection(correction);
		return rule;
	}

	private RangeConstraint createRule(int min, int max) {
		return this.createRule(min, max, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected RangeConstraint createDefaultInstance(int index) {
		RangeConstraint rule = this.createRule(index, index + 100,
				new DefaultValueCorrection(IntNode.valueOf(index)));
		return rule;
	}

	@Test
	public void shouldValidateCorrectValue() {
		RangeConstraint rule = this.createRule(100, 200);
		Assert.assertTrue(rule.validate(IntNode.valueOf(150)));
	}

	@Test
	public void shouldNotValidateWrongValue() {
		RangeConstraint rule = this.createRule(100, 200);
		Assert.assertFalse(rule.validate(IntNode.valueOf(250)));
	}

	@Test
	public void shouldRemoveWrongValue() {
		RangeConstraint rule = this.createRule(100, 200);
		Assert.assertEquals(FilterRecord.Instance,
				rule.fix(IntNode.valueOf(250)));
	}

	@Test
	public void shouldCorrectWrongValueGreaterMax() {
		RangeConstraint rule = this.createRule(100, 200,
				CleansFunctions.CHOOSE_NEAREST_BOUND);
		Assert.assertEquals(IntNode.valueOf(200),
				rule.fix(IntNode.valueOf(250)));
	}

	@Test
	public void shouldCorrectWrongValueSmallerMin() {
		RangeConstraint rule = this.createRule(100, 200,
				CleansFunctions.CHOOSE_NEAREST_BOUND);
		Assert.assertEquals(IntNode.valueOf(100), rule.fix(IntNode.valueOf(50)));
	}
}
