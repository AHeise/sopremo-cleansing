package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class CleansingRule<ContextType extends EvaluationContext> extends EvaluationExpression {

	@SuppressWarnings("unchecked")
	protected ContextType getContext() {
		return (ContextType) SopremoEnvironment.getInstance().getEvaluationContext();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.getClass().getSimpleName());
	}
}