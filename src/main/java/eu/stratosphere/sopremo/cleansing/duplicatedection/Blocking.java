package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.List;

import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.join.EquiJoin;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;

@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = "blocking")
public class Blocking extends MultipassDuplicateDetectionAlgorithm {
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.MultipassDuplicateDetectionAlgorithm#createPass(java.util.
	 * List, eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.PairFilter,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected Operator<?> createPass(List<Operator<?>> inputs, Pass pass, PairFilter pairFilter,
			CandidateComparison comparison) {
		final EquiJoin join = new EquiJoin().
			withKeyExpression(0, pass.getBlockingKey(0)).
			withKeyExpression(1, pass.getBlockingKey(1)).
			withInputs(inputs.get(0), inputs.get(inputs.size() - 1));
		return new Selection().
			withCondition(getCondition()).
			withResultProjection(comparison.getResultProjectionWithSimilarity()).
			withInputs(join);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm#requiresEnumeration()
	 */
	@Override
	protected boolean requiresEnumeration() {
		return true;
	}
}
