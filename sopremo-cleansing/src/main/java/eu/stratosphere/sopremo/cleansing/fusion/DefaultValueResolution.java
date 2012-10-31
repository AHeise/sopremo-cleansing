package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class DefaultValueResolution extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3038287806909149202L;

	private final IJsonNode defaultValue;

	public DefaultValueResolution(final IJsonNode defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public void fuse(final IArrayNode values, final double[] weights, final FusionContext context) {
		values.clear();
		values.add(this.defaultValue);
	}
}
