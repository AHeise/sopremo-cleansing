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
	public static Map<String, Class<? extends IJsonNode>> availableTypes = new HashMap<String, Class<? extends IJsonNode>>();

	static {
		availableTypes.put("int", IntNode.class);
		availableTypes.put("long", LongNode.class);
		availableTypes.put("bigint", BigIntegerNode.class);

		availableTypes.put("double", DoubleNode.class);
		availableTypes.put("decimal", DecimalNode.class);

		availableTypes.put("numeric", INumericNode.class);
		availableTypes.put("text", TextNode.class);
		availableTypes.put("bool", BooleanNode.class);
		availableTypes.put("array", IArrayNode.class);
		availableTypes.put("object", IObjectNode.class);
	}

	// private static List<Class<? extends IJsonNode>> buildList(
	// Class<? extends IJsonNode>... allowedClasses) {
	// List<Class<? extends IJsonNode>> classes = new LinkedList<Class<? extends
	// IJsonNode>>();
	// for (Class<? extends IJsonNode> nodeClass : allowedClasses) {
	// classes.add(nodeClass);
	// }
	// return classes;
	// }

	private final Class<? extends IJsonNode> type;

	public TypeConstraint(final Class<? extends IJsonNode> type) {
		this.type = type;
	}

	/**
	 * Initializes TypeValidationExpression.
	 * 
	 */
	TypeConstraint() {
		this.type = null;
	}

	private transient NodeCache nodeCache = new NodeCache();

	// @Override
	// public IJsonNode fix(final IJsonNode value) {
	// try {
	// if (value instanceof TextNode)
	// return LenientParser.INSTANCE.parse((TextNode) value,
	// this.type, LenientParser.ELIMINATE_NOISE);
	// return TypeCoercer.INSTANCE
	// .coerce(value, this.nodeCache, this.type);
	// } catch (final Exception e) {
	// return super.fix(value);
	// }
	// }

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
