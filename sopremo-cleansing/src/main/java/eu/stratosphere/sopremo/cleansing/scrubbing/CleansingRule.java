package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class CleansingRule extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1801909303463739160L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.getClass().getSimpleName());
	}
}