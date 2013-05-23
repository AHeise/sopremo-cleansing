package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;

public class RangeRule extends ValidationRule {
	private static final ValueCorrection CHOOSE_NEAREST_BOUND = new ValueCorrection() {
		private Object readResolve() {
			return CHOOSE_NEAREST_BOUND;
		}

		@Override
		public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
			final RangeRule that = (RangeRule) violatedRule;
			if (that.min.compareTo(value) > 0)
				return that.min;
			return that.max;
		}
	};

	private IJsonNode min, max;

	public RangeRule(final IJsonNode min, final IJsonNode max) {
		this.min = min;
		this.max = max;
		this.setValueCorrection(CHOOSE_NEAREST_BOUND);
	}
	
	/**
	 * Initializes RangeRule.
	 *
	 */
	RangeRule() {
	}

	@Override
	protected boolean validate(final IJsonNode value) {
		return this.min.compareTo(value) <= 0 && value.compareTo(this.max) <= 0;
	}
}
