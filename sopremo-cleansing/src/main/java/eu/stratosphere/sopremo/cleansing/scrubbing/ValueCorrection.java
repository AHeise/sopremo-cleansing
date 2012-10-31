package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ValueCorrection extends CleansingRule<ValidationContext> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5426600708331727317L;

	@Override
	public IJsonNode evaluateRule(final IJsonNode node, IJsonNode target, final ValidationContext context) {
		return this.fix(node,target, context);
	}

	public abstract IJsonNode fix(IJsonNode value, IJsonNode target, ValidationContext context);
}
