package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = "blocking")
public class Blocking extends CompositeDuplicateDetectionAlgorithm<Blocking> {

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
		if (selection.getPasses().size() == 1)
			return createPass(selection.getPasses().get(0), comparison).withInputs(inputs);

		List<Operator<?>> passes = new ArrayList<Operator<?>>();
		for (Pass pass : selection.getPasses())
			passes.add(createPass(pass, comparison).withInputs(inputs));

		return new Union().withInputs(passes);
	}

	private Operator<?> createPass(Pass pass, CandidateComparison comparison) {
		return new DirectBlocking().
			withKeyExpression(0, pass.getBlockingKeys().get(0)).
			withKeyExpression(1, pass.getBlockingKeys().get(comparison.isInnerSource() ? 0 : 1)).
			withCondition(comparison.asCondition()).
			withResultProjection(comparison.getResultProjectionWithSimilarity());
	}

	@InputCardinality(2)
	public static class DirectBlocking extends ElementaryDuplicateDetectionAlgorithm<DirectBlocking> {
		public static class Implementation extends SopremoMatch {
			private BooleanExpression condition;

			private transient IArrayNode<IJsonNode> pair = new ArrayNode<IJsonNode>();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.TypedSopremoMatch#match(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(IJsonNode left, IJsonNode right, JsonCollector collector) {
				this.pair.set(0, left);
				this.pair.set(1, right);
				if (this.condition.evaluate(this.pair) == BooleanNode.TRUE)
					collector.collect(this.pair);
			}
		}
	}
}
