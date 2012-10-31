package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.fusion.UnresolvableEvaluationException;
import eu.stratosphere.sopremo.type.IJsonNode;

public class UnresolvableCorrection extends ValueCorrection {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4543623927861750109L;

	/**
	 * The default, stateless instance.
	 */
	public final static UnresolvableCorrection INSTANCE = new UnresolvableCorrection();

	private Object readResolve() {
		return INSTANCE;
	}

	@Override
	public IJsonNode fix(final IJsonNode value, final IJsonNode target, final ValidationContext context) {
		throw new UnresolvableEvaluationException(String.format("Cannot fix %s voilating %s", value,
			context.getViolatedRule()));
	}

}
