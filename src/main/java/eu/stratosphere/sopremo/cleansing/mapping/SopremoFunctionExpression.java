package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.paths.PathExpression;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Predicates;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;

public class SopremoFunctionExpression extends Expression {
	private EvaluationExpression expr;

	private List<PathExpression> inputPaths;

	/**
	 * Returns the expr.
	 * 
	 * @return the expr
	 */
	public EvaluationExpression getExpr() {
		return this.expr;
	}

	/*
	 * (non-Javadoc)
	 * @see it.unibas.spicy.model.expressions.Expression#toString()
	 */
	public String toDebugString() {
		return this.expr.clone().replace(Predicates.instanceOf(InputSelection.class),
			new Function<EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression apply(EvaluationExpression input) {
					return new ConstantExpression(SopremoFunctionExpression.this.inputPaths.get(
						((InputSelection) input).getIndex()).toString());
				}
			}).toString();
	}

	/*
	 * (non-Javadoc)
	 * @see it.unibas.spicy.model.expressions.Expression#toString()
	 */
	@Override
	public String toString() {
		return "0";
	}

	/*
	 * (non-Javadoc)
	 * @see it.unibas.spicy.model.expressions.Expression#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		// hack for weird Spicy / JEP checks
		if (obj instanceof Expression)
			return true;
		return super.equals(obj);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return super.hashCode();
	}

	public SopremoFunctionExpression(final EvaluationExpression expr, List<PathExpression> sourcePaths) {
		super("0");
		this.expr = expr;
		this.inputPaths = sourcePaths;
	}

}
