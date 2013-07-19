package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;

public class RangeRule extends ValidationRule {
	private IJsonNode min, max;

	public RangeRule(final IJsonNode min, final IJsonNode max) {
		this.min = min;
		this.max = max;
	}
	
	/**
	 * Initializes RangeRule.
	 *
	 */
	RangeRule() {
	}

	@Override
	public boolean validate(final IJsonNode value) {
		return this.min.compareTo(value) <= 0 && value.compareTo(this.max) <= 0;
	}
	
	public IJsonNode getMin() {
		return min;
	}

	public IJsonNode getMax() {
		return max;
	}
}
