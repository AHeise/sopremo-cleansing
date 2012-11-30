package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.PactBuilderUtil;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

public class SortedNeighborhood extends CompositeDuplicateDetectionAlgorithm<SortedNeighborhood> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9162693656982906448L;
	private int windowSize = 10;

	/**
	 * Sets the windowSize to the specified value.
	 * 
	 * @param windowSize
	 *        the windowSize to set
	 */
	@Property
	public void setWindowSize(int windowSize) {
		if (windowSize <= 0)
			throw new IllegalArgumentException("windowSize must be positive");

		this.windowSize = windowSize;
	}

	/**
	 * Returns the windowSize.
	 * 
	 * @return the windowSize
	 */
	public int getWindowSize() {
		return this.windowSize;
	}

	/**
	 * Sets the windowSize to the specified value.
	 * 
	 * @param windowSize
	 *        the windowSize to set
	 */
	public SortedNeighborhood withWindowSize(int windowSize) {
		setWindowSize(windowSize);
		return self();
	}

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
		return new SinglePassIntraSourceSNM().withComparison(comparison).withKeyExpressions(0, blockingKeys);
	}

	@InputCardinality(min = 1, max = 1)
	public static class SinglePassIntraSourceSNM extends
			ElementaryDuplicateDetectionAlgorithm<SinglePassIntraSourceSNM> {
		private int windowSize = 10;

		/**
		 * Sets the windowSize to the specified value.
		 * 
		 * @param windowSize
		 *        the windowSize to set
		 */
		@Property
		public void setWindowSize(int windowSize) {
			if (windowSize <= 0)
				throw new IllegalArgumentException("windowSize must be positive");

			this.windowSize = windowSize;
		}

		/**
		 * Returns the windowSize.
		 * 
		 * @return the windowSize
		 */
		public int getWindowSize() {
			return this.windowSize;
		}

		/**
		 * Sets the windowSize to the specified value.
		 * 
		 * @param windowSize
		 *        the windowSize to set
		 */
		public SinglePassIntraSourceSNM withWindowSize(int windowSize) {
			setWindowSize(windowSize);
			return self();
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 8219222010267680701L;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.operator.ElementaryOperator#getContract(eu.stratosphere.sopremo.serialization.Schema)
		 */
		@Override
		protected Contract getContract(Schema globalSchema) {
			int[] keyIndices = this.getKeyIndices(globalSchema, this.getKeyExpressions(0));
			ReduceContract.Builder builder = ReduceContract.builder(Implementation.class);
			builder.name(this.toString());

			final Class<? extends Key>[] keyClasses = this.getKeyClasses(globalSchema, keyIndices);
			PactBuilderUtil.addKeys(builder, keyClasses, keyIndices);

			final Ordering order = new Ordering();
			for (int index = 0; index < keyIndices.length; index++)
				order.appendOrdering(keyIndices[index], keyClasses[index], Order.ASCENDING);

			builder.secondaryOrder(order);
			return builder.build();
		}

		public static final class Implementation extends SopremoReduce {
			private int windowSize;
			private CircularArrayNode buffer;
			private CandidateComparison comparison;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#open(eu.stratosphere.nephele.configuration.Configuration)
			 */
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				buffer = new CircularArrayNode(windowSize);
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IStreamArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(IStreamArrayNode values, JsonCollector out) {
				for (IJsonNode value : values) {
					for (IJsonNode bufferedValue : buffer) {
						comparison.process(value, bufferedValue, out);
					}
					buffer.add(value);
				}
			}
		}
	}

}