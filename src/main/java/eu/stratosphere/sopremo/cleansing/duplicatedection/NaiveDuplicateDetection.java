package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.join.ThetaJoin;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;

@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = "naive")
public class NaiveDuplicateDetection extends CompositeDuplicateDetectionAlgorithm<NaiveDuplicateDetection> {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm#getImplementation(java
	 * .util.List, eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected Operator<?> getImplementation(List<Operator<?>> inputs, CandidateSelection selection,
			CandidateComparison comparison, EvaluationContext context) {
		return new ThetaJoin().
			withCondition(comparison.asCondition()).
			withResultProjection(comparison.getResultProjectionWithSimilarity()).
			withInputs(inputs);
	}
	//
	// @InputCardinality(2)
	// public static class DirectNaiveDuplicateDetection extends
	// ElementaryDuplicateDetectionAlgorithm<DirectNaiveDuplicateDetection> {
	// public static class Implementation extends SopremoCross {
	// private CandidateComparison comparison;
	//
	// @Override
	// protected void cross(IJsonNode left, IJsonNode right, JsonCollector collector) {
	// this.comparison.process(left, right, collector);
	// }
	// }
	// }
}
