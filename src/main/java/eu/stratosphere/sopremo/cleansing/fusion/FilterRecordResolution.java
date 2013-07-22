package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class FilterRecordResolution extends ConflictResolution {
	@Override
	public void fuse(final IArrayNode<IJsonNode> values, final double[] weights) {
		throw new UnresolvableEvaluationException();
	}
}
