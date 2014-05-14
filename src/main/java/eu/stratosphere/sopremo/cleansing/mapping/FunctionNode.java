package eu.stratosphere.sopremo.cleansing.mapping;

import org.nfunk.jep.ASTFunNode;

import com.google.common.base.Function;
import com.google.common.base.Predicates;

import eu.stratosphere.sopremo.expressions.*;

public class FunctionNode extends ASTFunNode {

	private EvaluationExpression expr;

	public FunctionNode(final int id) {
		super(id);
	}

	public FunctionNode(final int id, final EvaluationExpression expr) {
		super(id);
		if (expr instanceof FunctionCall)
			for (final EvaluationExpression param : ((FunctionCall) expr).getParameters())
				param.replace(Predicates.instanceOf(InputSelection.class),
					new Function<EvaluationExpression, EvaluationExpression>() {
						@Override
						public EvaluationExpression apply(final EvaluationExpression ee) {
							final InputSelection is = (InputSelection) ee;
							final EvaluationExpression oa =
								new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
							return oa;
						}
					});
		else if (expr instanceof ArrayAccess)
			((ArrayAccess) expr).replace(Predicates.instanceOf(InputSelection.class),
				new Function<EvaluationExpression, EvaluationExpression>() {
					@Override
					public EvaluationExpression apply(final EvaluationExpression ee) {
						final InputSelection is = (InputSelection) ee;
						final EvaluationExpression oa =
							new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
						return oa;
					}
				});
		else if (expr instanceof TernaryExpression || expr instanceof ObjectCreation || expr instanceof ArrayCreation)
			for (final ObjectAccess oa : expr.findAll(ObjectAccess.class))
				oa.replace(Predicates.instanceOf(InputSelection.class),
					new Function<EvaluationExpression, EvaluationExpression>() {
						@Override
						public EvaluationExpression apply(final EvaluationExpression ee) {
							final InputSelection is = (InputSelection) ee;
							final EvaluationExpression oa =
								new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
							return oa;
						}
					});
		this.expr = expr;
	}

	public EvaluationExpression getExpression() {
		return this.expr;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.expr == null ? 0 : this.expr.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final FunctionNode other = (FunctionNode) obj;
		if (this.expr == null) {
			if (other.expr != null)
				return false;
		} else if (!this.expr.equals(other.expr))
			return false;
		return true;
	}
}
