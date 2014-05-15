package eu.stratosphere.sopremo.cleansing.scrubbing;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

public class NonNullConstraintTest extends EqualCloneTest<NonNullConstraint> {

	private final IJsonNode NON_NULL = IntNode.ONE;

	private final IJsonNode NULL = NullNode.getInstance();

	private final IJsonNode MISSING = MissingNode.getInstance();

	@Override
	protected NonNullConstraint createDefaultInstance(int index) {
		return new NonNullConstraint(IntNode.valueOf(index));
	}

	@Test
	public void shouldValidateNonNullValues() {
		Assert.assertTrue(new NonNullConstraint().validate(this.NON_NULL));
	}

	@Test
	public void shouldNotValidateNullValues() {
		Assert.assertFalse(new NonNullConstraint().validate(this.NULL));
	}

	@Test
	public void shouldNotValidateMissingValues() {
		Assert.assertFalse(new NonNullConstraint().validate(this.MISSING));
	}

	@Test
	public void shouldRemoveNullValues() {
		Assert.assertEquals(FilterRecord.Instance, new NonNullConstraint().fix(this.NULL));
	}

	@Test
	public void shouldReplaceNullValuesWithDefault() {
		Assert.assertEquals(this.NON_NULL, new NonNullConstraint(this.NON_NULL).fix(this.NULL));
	}

}
