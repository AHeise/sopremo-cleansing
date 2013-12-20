package eu.stratosphere.sopremo.cleansing.mapping;

import org.nfunk.jep.ASTFunNode;

import eu.stratosphere.sopremo.expressions.FunctionCall;

public class FunctionNode extends ASTFunNode{
	
	private FunctionCall function;

	public FunctionNode(int id) {
		super(id);
	}
	
	public FunctionNode(int id, FunctionCall function) {
		super(id);
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
