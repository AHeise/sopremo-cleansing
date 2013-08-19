package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

public class NonNullRuleTest extends EqualCloneTest<NonNullRule> {

	private final IJsonNode NON_NULL = IntNode.ONE;
	private final IJsonNode NULL = NullNode.getInstance();
	private final IJsonNode MISSING = MissingNode.getInstance();

	@Override
	protected NonNullRule createDefaultInstance(int index) {
		return new NonNullRule(IntNode.valueOf(index));
	}

	@Test
	public void shouldValidateNonNullValues() {
		Assert.assertTrue(new NonNullRule().validate(NON_NULL));
	}

	@Test
	public void shouldNotValidateNullValues() {
		Assert.assertFalse(new NonNullRule().validate(NULL));
	}

	@Test
	public void shouldNotValidateMissingValues() {
		Assert.assertFalse(new NonNullRule().validate(MISSING));
	}

	@Test
	public void shouldRemoveNullValues() {
		Assert.assertEquals(FilterRecord.Instance, new NonNullRule().fix(NULL));
	}

	@Test
	public void shouldReplaceNullValuesWithDefault() {
		Assert.assertEquals(NON_NULL, new NonNullRule(NON_NULL).fix(NULL));
	}

}
