package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TypeNode;

@Name(adjective = "type")
public class TypeConstraint extends ValidationRule {
	public static Map<String, TypeNode> AvailableTypes =
		new HashMap<String, TypeNode>();

	private final TypeNode type;

	public TypeConstraint(final TypeNode params) {
		this.type = params;
		setValueCorrection(CleansFunctions.TRY_COERCING_TO_TYPE);
	}

	/**
	 * Initializes TypeValidationExpression.
	 */
	TypeConstraint() {
		this(null);
	}

	private transient NodeCache nodeCache = new NodeCache();

	@Override
	public boolean validate(final IJsonNode value) {
		return this.type.getNodeType().equals(value.getType());
	}

	public TypeNode getType() {
		return this.type;
	}

	public NodeCache getNodeCache() {
		return this.nodeCache;
	}

	@Override
	public String toString() {
		return "TypeValidationExpression [type=" + this.type + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		TypeConstraint other = (TypeConstraint) obj;
		return this.type.equals(other.type);
	}

}
