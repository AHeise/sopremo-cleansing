package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class BlackListRuleTest extends EqualCloneTest<BlackListRule> {

	private final IJsonNode V1 = IntNode.valueOf(100);
	private final IJsonNode V2 = IntNode.valueOf(200);
	private final IJsonNode V3 = IntNode.valueOf(300);
	private final IJsonNode V_CORRECT = IntNode.valueOf(400);

	private final List<IJsonNode> blacklist = new ArrayList<IJsonNode>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7331117558623222173L;

		{
			this.add(V1);
			this.add(V2);
			this.add(V3);
		}
	};

	private BlackListRule createRule(List<IJsonNode> list,
			ValueCorrection correction) {
		BlackListRule rule = new BlackListRule(list);
		rule.setValueCorrection(correction);
		return rule;
	}

	private BlackListRule createRule(List<IJsonNode> list) {
		return this.createRule(list, ValidationRule.DEFAULT_CORRECTION);
	}

	@Override
	protected BlackListRule createDefaultInstance(int index) {
		List<IJsonNode> list = new ArrayList<IJsonNode>();
		list.add(IntNode.valueOf(index));
		BlackListRule rule = this.createRule(list);
		return rule;
	}

	@Test
	public void shouldValidateCorrectValue() {
		BlackListRule rule = this.createRule(this.blacklist);
		Assert.assertTrue(rule.validate(V_CORRECT));
	}

	@Test
	public void shouldNotValidateWrongValue() {
		BlackListRule rule = this.createRule(this.blacklist);
		Assert.assertFalse(rule.validate(V2));
	}

	@Test
	public void shouldRemoveWrongValue() {
		BlackListRule rule = this.createRule(this.blacklist);
		Assert.assertEquals(FilterRecord.Instance, rule.fix(V3));
	}
}
