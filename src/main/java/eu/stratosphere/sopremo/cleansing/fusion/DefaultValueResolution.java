package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class DefaultValueResolution extends ConflictResolution {
	private final IJsonNode defaultValue;

	public DefaultValueResolution() {
		defaultValue = NullNode.getInstance();
	}
	
	public DefaultValueResolution(final IJsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public void fuse(final IArrayNode<IJsonNode> values, final double[] weights) {
		values.clear();
		values.add(this.defaultValue);
	}
}
