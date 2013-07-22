package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ValueCorrection extends CleansingRule<ValidationContext> {
	@Override
	public IJsonNode evaluate(final IJsonNode node) {
		return this.fix(node, this.getContext().getViolatedRule());
	}

	public abstract IJsonNode fix(IJsonNode value, ValidationRule valiotedRule);
}
