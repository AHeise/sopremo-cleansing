package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class WhiteListConstraintTest extends EqualCloneTest<WhiteListConstraint> {

	private final IJsonNode V1 = IntNode.valueOf(100);

	private final IJsonNode V2 = IntNode.valueOf(200);

	private final IJsonNode V3 = IntNode.valueOf(300);

	private final IJsonNode V_WRONG = IntNode.valueOf(400);

	private final List<IJsonNode> whitelist = new ArrayList<IJsonNode>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2531859104149221143L;

		{
			this.add(WhiteListConstraintTest.this.V1);
			this.add(WhiteListConstraintTest.this.V2);
			this.add(WhiteListConstraintTest.this.V3);
		}
	};

	private WhiteListConstraint createRule(List<IJsonNode> list,
			ValueCorrection correction) {
		WhiteListConstraint rule = new WhiteListConstraint(list);
		rule.setValueCorrection(correction);
		return rule;
	}

	private WhiteListConstraint createRule(List<IJsonNode> list) {
		return this.createRule(list, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected WhiteListConstraint createDefaultInstance(int index) {
		List<IJsonNode> list = new ArrayList<IJsonNode>();
		list.add(IntNode.valueOf(index));
		WhiteListConstraint rule = this.createRule(list);
		return rule;
	}

	@Test
	public void shouldValidateCorrectValue() {
		WhiteListConstraint rule = this.createRule(this.whitelist);
		Assert.assertTrue(rule.validate(this.V2));
	}

	@Test
	public void shouldNotValidateWrongValue() {
		WhiteListConstraint rule = this.createRule(this.whitelist);
		Assert.assertFalse(rule.validate(this.V_WRONG));
	}

	@Test
	public void shouldRemoveWrongValue() {
		WhiteListConstraint rule = this.createRule(this.whitelist);
		Assert.assertEquals(FilterRecord.Instance, rule.fix(this.V_WRONG));
	}

	@Test
	public void shouldCorrectWrongValue() {
		WhiteListConstraint rule = this.createRule(this.whitelist,
			CleansFunctions.CHOOSE_FIRST_FROM_LIST);
		Assert.assertEquals(this.V1, rule.fix(this.V_WRONG));
	}
}
