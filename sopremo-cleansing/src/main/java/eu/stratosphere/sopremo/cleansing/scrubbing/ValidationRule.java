package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ValidationRule extends CleansingRule<ValidationContext> {
	public static final UnresolvableCorrection DEFAULT_CORRECTION = UnresolvableCorrection.INSTANCE;

	/**
	 * 
	 */
	private static final long serialVersionUID = -7139939245760263511L;

	private ValueCorrection valueCorrection = DEFAULT_CORRECTION;

	@Override
	public IJsonNode evaluateRule(IJsonNode value, IJsonNode target, ValidationContext context) {
		if (!this.validate(value, context)) {
			context.setViolatedRule(this);
			return this.fix(value, target, context);
		}
		return value;
	}

	protected IJsonNode fix(final IJsonNode value, IJsonNode target, final ValidationContext context) {
		return this.valueCorrection.fix(value, target,  context);
	}

	public ValueCorrection getValueCorrection() {
		return this.valueCorrection;
	}

	public void setValueCorrection(final ValueCorrection valueCorrection) {
		if (valueCorrection == null)
			throw new NullPointerException("valueCorrection must not be null");

		this.valueCorrection = valueCorrection;
	}

	protected boolean validate(final IJsonNode value, final ValidationContext context) {
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.valueCorrection.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		ValidationRule other = (ValidationRule) obj;
		return this.valueCorrection.equals(other.valueCorrection);
	}

}
