package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class FilterRecordResolution extends ConflictResolution<IJsonNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1764171809609427171L;

	/**
	 * The default, stateless instance.
	 */
	public final static FilterRecordResolution INSTANCE = new FilterRecordResolution();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values) {
		throw new UnresolvableEvaluationException();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#createCopy()
	 */
	@Override
	protected EvaluationExpression createCopy() {
		return INSTANCE;
	}
}
