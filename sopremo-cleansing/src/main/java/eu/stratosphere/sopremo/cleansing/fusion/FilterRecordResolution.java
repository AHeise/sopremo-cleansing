package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.type.IArrayNode;

public class FilterRecordResolution extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1764171809609427171L;

	@Override
	public void fuse(final IArrayNode values, final double[] weights, final FusionContext context) {
		throw new UnresolvableEvaluationException();
	}
}
