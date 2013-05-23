package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.type.IJsonNode;

public class UnresolvableCorrection extends ValueCorrection {
	/**
	 * The default, stateless instance.
	 */
	public final static UnresolvableCorrection INSTANCE = new UnresolvableCorrection();

	private Object readResolve() {
		return INSTANCE;
	}

	@Override
	public IJsonNode fix(final IJsonNode value, ValidationRule violatedRule) {
		return FilterRecord.Instance;
//		throw new UnresolvableEvaluationException(String.format("Cannot fix %s voilating %s", value, violatedRule));
	}

}
