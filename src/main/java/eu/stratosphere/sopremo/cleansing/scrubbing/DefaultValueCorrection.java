package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

@Name(preposition="default")
public class DefaultValueCorrection extends ValueCorrection {
	/**
	 * The default, stateless instance.
	 */
	public final static DefaultValueCorrection SET_NULL = new DefaultValueCorrection(NullNode.getInstance());

	private IJsonNode defaultValue;

	public DefaultValueCorrection(final IJsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	/**
	 * Initializes DefaultValueCorrection.
	 *
	 */
	DefaultValueCorrection() {
		this.defaultValue = null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.scrubbing.ValueCorrection#fix(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.cleansing.scrubbing.ValidationContext)
	 */
	@Override
	public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
		return this.defaultValue;
	}

}
