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

public class WhiteListRuleTest extends EqualCloneTest<WhiteListRule> {

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
			this.add(V1);
			this.add(V2);
			this.add(V3);
		}
	};

	private WhiteListRule createRule(List<IJsonNode> list,
			ValueCorrection correction) {
		WhiteListRule rule = new WhiteListRule(list);
		rule.setValueCorrection(correction);
		return rule;
	}

	private WhiteListRule createRule(List<IJsonNode> list) {
		return this.createRule(list, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected WhiteListRule createDefaultInstance(int index) {
		List<IJsonNode> list = new ArrayList<IJsonNode>();
		list.add(IntNode.valueOf(index));
		WhiteListRule rule = this.createRule(list);
		return rule;
	}

	@Test
	public void shouldValidateCorrectValue() {
		WhiteListRule rule = this.createRule(this.whitelist);
		Assert.assertTrue(rule.validate(V2));
	}

	@Test
	public void shouldNotValidateWrongValue() {
		WhiteListRule rule = this.createRule(this.whitelist);
		Assert.assertFalse(rule.validate(V_WRONG));
	}

	@Test
	public void shouldRemoveWrongValue() {
		WhiteListRule rule = this.createRule(this.whitelist);
		Assert.assertEquals(FilterRecord.Instance, rule.fix(V_WRONG));
	}

	@Test
	public void shouldCorrectWrongValue() {
		WhiteListRule rule = this.createRule(this.whitelist,
				CleansFunctions.CHOOSE_FIRST_FROM_LIST);
		Assert.assertEquals(V1, rule.fix(V_WRONG));
	}
}
