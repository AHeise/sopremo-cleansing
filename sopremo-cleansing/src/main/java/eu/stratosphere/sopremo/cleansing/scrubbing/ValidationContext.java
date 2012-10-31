package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;

public class ValidationContext extends EvaluationContext {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3830001019910981066L;

	private transient ValidationRule violatedRule;

	private transient IJsonNode contextNode;

	public ValidationContext(final EvaluationContext context) {
		super(context);
	}

	public IJsonNode getContextNode() {
		return this.contextNode;
	}

	public ValidationRule getViolatedRule() {
		return this.violatedRule;
	}

	public void setContextNode(final IJsonNode contextNode) {
		if (contextNode == null)
			throw new NullPointerException("contextNode must not be null");

		this.contextNode = contextNode;
	}

	public void setViolatedRule(final ValidationRule violatedRule) {
		if (violatedRule == null)
			throw new NullPointerException("violatedRule must not be null");

		this.violatedRule = violatedRule;
	}

}
