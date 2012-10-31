package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class CleansingRule<ContextType extends EvaluationContext> extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1801909303463739160L;

	@SuppressWarnings("unchecked")
	@Override
	public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
		return this.evaluateRule(node, target, (ContextType) context);
	}

	public abstract IJsonNode evaluateRule(IJsonNode node, IJsonNode target, ContextType context);

	@Override
	public void toString(StringBuilder builder) {
		builder.append(this.getClass().getSimpleName());
	}
}