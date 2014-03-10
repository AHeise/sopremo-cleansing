package eu.stratosphere.sopremo.cleansing.mapping;

import org.nfunk.jep.ASTFunNode;

import com.google.common.base.Function;
import com.google.common.base.Predicates;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.TernaryExpression;

public class FunctionNode extends ASTFunNode {

	private EvaluationExpression expr;

	public FunctionNode(int id) {
		super(id);
	}

	public FunctionNode(int id, EvaluationExpression expr) {
		super(id);
		if (expr instanceof FunctionCall) {
			for (EvaluationExpression param : ((FunctionCall) expr).getParameters()) {

				param.replace(Predicates.instanceOf(InputSelection.class), new Function<EvaluationExpression, EvaluationExpression>() {
					public EvaluationExpression apply(EvaluationExpression ee) {
						InputSelection is = (InputSelection) ee;
						EvaluationExpression oa = new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
						return oa;
					}
				});

			}
		} else if (expr instanceof ArrayAccess) {
			((ArrayAccess) expr).replace(Predicates.instanceOf(InputSelection.class), new Function<EvaluationExpression, EvaluationExpression>() {
				public EvaluationExpression apply(EvaluationExpression ee) {
					InputSelection is = (InputSelection) ee;
					EvaluationExpression oa = new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
					return oa;
				}
			});
		}

		else if (expr instanceof TernaryExpression || expr instanceof ObjectCreation || expr instanceof ArrayCreation) {
			for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
				oa.replace(Predicates.instanceOf(InputSelection.class), new Function<EvaluationExpression, EvaluationExpression>() {
					public EvaluationExpression apply(EvaluationExpression ee) {
						InputSelection is = (InputSelection) ee;
						EvaluationExpression oa = new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression());
						return oa;
					}
				});
			}
		}
		this.expr = expr;
	}

	public EvaluationExpression getExpression() {
		return expr;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((expr == null) ? 0 : expr.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FunctionNode other = (FunctionNode) obj;
		if (expr == null) {
			if (other.expr != null)
				return false;
		} else if (!expr.equals(other.expr))
			return false;
		return true;
	}
}
