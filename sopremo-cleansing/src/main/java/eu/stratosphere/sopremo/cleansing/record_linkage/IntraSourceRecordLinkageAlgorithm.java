package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.operator.Operator;

public interface IntraSourceRecordLinkageAlgorithm {
	public abstract Operator<?> getIntraSource(BooleanExpression duplicateCondition, RecordLinkageInput input);
}
