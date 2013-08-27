package eu.stratosphere.sopremo.cleansing.duplicatedection;


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
public class Blocking extends MultipassDuplicateDetectionAlgorithm {

	@Override
	protected Operator<?> createPass(Pass pass, CandidateComparison comparison) {
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
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoMatch#match(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(IJsonNode left, IJsonNode right, JsonCollector<IJsonNode> collector) {
				this.pair.set(0, left);
				this.pair.set(1, right);
				if (this.condition.evaluate(this.pair) == BooleanNode.TRUE)
					collector.collect(this.pair);
			}
		}
	}
}
