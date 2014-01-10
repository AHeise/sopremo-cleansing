package eu.stratosphere.sopremo.cleansing.mapping;

import org.nfunk.jep.ASTFunNode;

import com.google.common.base.Function;
import com.google.common.base.Predicates;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;

public class FunctionNode extends ASTFunNode{
	
	private FunctionCall function;

	public FunctionNode(int id) {
		super(id);
	}
	
	public FunctionNode(int id, FunctionCall function) {
		super(id);
		for(EvaluationExpression param : function.getParameters()){
			
			param.replace(Predicates.instanceOf(InputSelection.class), new Function<EvaluationExpression, EvaluationExpression>() {
				public EvaluationExpression apply(EvaluationExpression ee){
					InputSelection is = (InputSelection) ee;
					EvaluationExpression oa = new ArrayAccess(is.getIndex()).withInputExpression(is.getInputExpression()); 
					return oa;
				}
			});
			
		}
		this.function = function;
	}
	
	public FunctionCall getFunction() {
		return function;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((function == null) ? 0 : function.hashCode());
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
		if (function == null) {
			if (other.function != null)
				return false;
		} else if (!function.equals(other.function))
			return false;
		return true;
	}
}
