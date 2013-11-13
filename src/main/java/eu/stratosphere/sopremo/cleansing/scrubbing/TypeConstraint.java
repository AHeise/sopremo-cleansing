package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.TextNode;

@Name(adjective = "type")
public class TypeConstraint extends ValidationRule {
	public static Map<String, Class<? extends IJsonNode>> AvailableTypes =
		new HashMap<String, Class<? extends IJsonNode>>();

	static {
		AvailableTypes.put("int", IntNode.class);
		AvailableTypes.put("long", LongNode.class);
		AvailableTypes.put("bigint", BigIntegerNode.class);

		AvailableTypes.put("double", DoubleNode.class);
		AvailableTypes.put("decimal", DecimalNode.class);

		AvailableTypes.put("numeric", INumericNode.class);
		AvailableTypes.put("text", TextNode.class);
		AvailableTypes.put("bool", BooleanNode.class);
		AvailableTypes.put("array", IArrayNode.class);
		AvailableTypes.put("object", IObjectNode.class);
	}

	private final Class<? extends IJsonNode> type;

	public TypeConstraint(final Class<? extends IJsonNode> type) {
		this.type = type;
	}

	/**
	 * Initializes TypeValidationExpression.
	 */
	TypeConstraint() {
		this.type = null;
	}

	private transient NodeCache nodeCache = new NodeCache();

	@Override
	public boolean validate(final IJsonNode value) {
		return this.type.isInstance(value);
	}

	public Class<? extends IJsonNode> getType() {
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
