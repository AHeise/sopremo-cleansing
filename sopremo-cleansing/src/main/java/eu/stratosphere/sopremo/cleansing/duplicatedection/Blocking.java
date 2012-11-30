package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

public class Blocking extends CompositeDuplicateDetectionAlgorithm<Blocking> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9162693656982906448L;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm#getImplementation(java
	 * .util.List, java.util.List, eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected Operator<?> getImplementation(List<Operator<?>> inputs, List<EvaluationExpression> blockingKeys,
			CandidateComparison comparison, EvaluationContext context) {
		return new SinglePassIntraSourceBlocking().withComparison(comparison).withKeyExpressions(0, blockingKeys);
	}

	@InputCardinality(min = 1, max = 1)
	public static class SinglePassIntraSourceBlocking extends
			ElementaryDuplicateDetectionAlgorithm<SinglePassIntraSourceBlocking> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8219222010267680701L;

		public static final class Implementation extends SopremoReduce {
			private CandidateComparison comparison;

			private transient CachingArrayNode cachedNodes = new CachingArrayNode();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IStreamArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(IStreamArrayNode values, JsonCollector out) {
				for (IJsonNode value : values)
					this.cachedNodes.add(value);

				for (IJsonNode node1 : cachedNodes)
					for (IJsonNode node2 : cachedNodes)
						this.comparison.process(node1, node2, out);

				this.cachedNodes.clear();
			}
		}
	}

	@InputCardinality(min = 1, max = 1)
	public static class SinglePassInterSourceBlocking extends
			ElementaryDuplicateDetectionAlgorithm<SinglePassInterSourceBlocking> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8219222010267680701L;

		public static final class Implementation extends SopremoCoGroup {
			private CandidateComparison comparison;

			private transient CachingArrayNode cachedNodes = new CachingArrayNode();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamArrayNode,
			 * eu.stratosphere.sopremo.type.IStreamArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out) {
				// cache one side only
				for (IJsonNode value : values1)
					this.cachedNodes.add(value);

				for (IJsonNode node2 : values2)
					for (IJsonNode node1 : cachedNodes)
						this.comparison.process(node1, node2, out);

				this.cachedNodes.clear();
			}
		}
	}
}