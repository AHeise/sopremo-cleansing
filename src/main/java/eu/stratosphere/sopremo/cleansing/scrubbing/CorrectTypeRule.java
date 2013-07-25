package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

public class CorrectTypeRule extends ValidationRule {

	public static Map<String, Class<? extends IJsonNode>> types = new HashMap<String, Class<? extends IJsonNode>>() {
		private static final long serialVersionUID = -6887866026574883045L;

		{
			this.put("text", TextNode.class);
			this.put("array", IArrayNode.class);
			this.put("object", IObjectNode.class);
			this.put("int", IntNode.class);
			this.put("double", DoubleNode.class);
			this.put("long", LongNode.class);
			this.put("big_int", BigIntegerNode.class);
			this.put("decimal", DecimalNode.class);
			this.put("null", NullNode.class);
		}
	};
	private Class<? extends IJsonNode> expectedType;

	public CorrectTypeRule() {
		this.expectedType = null;
	}

	public CorrectTypeRule(Class<? extends IJsonNode> expectedType) {
		this.expectedType = expectedType;
	}

	@Override
	public boolean validate(final IJsonNode node) {
		return node.getClass().equals(this.expectedType);
	}

}
