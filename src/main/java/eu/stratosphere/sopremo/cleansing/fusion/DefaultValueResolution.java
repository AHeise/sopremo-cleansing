package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Map;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

@Name(noun = "defaultResolution")
@SingleOutputResolution
public class DefaultValueResolution extends ConflictResolution {
	private final IJsonNode defaultValue;

	public DefaultValueResolution() {
		defaultValue = NullNode.getInstance();
	}

	public DefaultValueResolution(final IJsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public void fuse(final IArrayNode<IJsonNode> values,
			Map<String, CompositeEvidence> weights) {
		values.clear();
		values.add(this.defaultValue);
	}
}
