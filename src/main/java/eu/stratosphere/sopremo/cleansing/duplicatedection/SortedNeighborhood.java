/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javolution.util.FastList;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.ContextualProjection;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Sort;
import eu.stratosphere.sopremo.base.join.EquiJoin;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.OrderingExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.GenericSopremoReduce;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * @author Arvid Heise
 */
@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = { "snm", "sorted neighborhood" })
public class SortedNeighborhood extends MultipassDuplicateDetectionAlgorithm {
	private static final int DEFAULT_WINDOW_SIZE = 2, DEFAULT_NUMBER_OF_PARTITION = 10;

	private int windowSize = DEFAULT_WINDOW_SIZE;

	private int numberOfPartitions = DEFAULT_NUMBER_OF_PARTITION;

	/**
	 * [sorting key, count] -&gt; [key, rank]
	 */
	public static class EnumerationExpression extends EvaluationExpression {
		private int rank = 0;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		public IJsonNode evaluate(IJsonNode node) {
			@SuppressWarnings("unchecked")
			final IntNode rankNode = ((IArrayNode<IntNode>) node).get(1);
			this.rank += rankNode.getIntValue();
			rankNode.setValue(this.rank);
			return node;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.MultipassDuplicateDetectionAlgorithm#createPass(java.util.
	 * List, eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected Operator<?> createPass(List<Operator<?>> inputs, Pass pass, CandidateComparison comparison) {
		if (!comparison.isInnerSource())
			throw new UnsupportedOperationException();

		// record -> sorting key
		final Projection sortingKeys = new Projection().
			withInputs(inputs.get(0)).
			withResultProjection(new ArrayCreation(
				pass.getBlockingKeys().get(0),
				new ReadConfigurationExpression("pact.parallel.task.id", new IntNode())));

		// sorting key+ -> [sorting key, count]
		final BatchAggregationExpression countExpression = new BatchAggregationExpression();
		final Grouping sortingKeyCount = new Grouping().
			withInputs(sortingKeys).
			withGroupingKey(EvaluationExpression.VALUE).
			withResultProjection(
				new ArrayCreation().
					add(countExpression.add(CoreFunctions.FIRST)).
					add(countExpression.add(CoreFunctions.COUNT))).
			withName("Generate KPM List " + pass.getBlockingKeys().get(0));

		// [sorting key, count] -> [sorting key, rank]
		final Sort keysWithRank = new Sort().
			withInputs(sortingKeyCount).
			withResultProjection(new EnumerationExpression());

		final Grouping countRecords = new Grouping().
			withInputs(sortingKeys).
			withResultProjection(CoreFunctions.COUNT.inline(EvaluationExpression.VALUE));
		// [sorting key, rank] -> [sorting key, rank, count]
		final ContextualProjection keysWithRankAndCount = new ContextualProjection().
			withInputs(keysWithRank, countRecords).
			withContextPath(new ArrayAccess(2));

		// record + [sorting key, rank, count] -> [partition, partition, record]{1,3}
		final PartitionKeys rankJoin = new PartitionKeys(this.windowSize, this.numberOfPartitions).
			withInputs(inputs.get(0), keysWithRankAndCount).
			withKeyExpression(0, pass.getBlockingKeys().get(0)).
			withKeyExpression(1, new ArrayAccess(0));

		final SlidingWindow window = new SlidingWindow().
			withInputs(rankJoin).
			withWindowSize(this.windowSize).withCondition(comparison.asCondition()).
			withKeyExpression(0, new ArrayAccess(0)).
			withInnerGroupOrdering(0, new OrderingExpression(Order.ASCENDING, new ArrayAccess(2)));

		return window;
	}

	@InputCardinality(1)
	public static class SlidingWindow extends ElementaryOperator<SlidingWindow> {
		private int windowSize = DEFAULT_WINDOW_SIZE;

		private BooleanExpression condition = new UnaryExpression(new ConstantExpression(true));

		/**
		 * Initializes SortedNeighborhood.SlidingWindow.
		 */
		public SlidingWindow() {
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
		public void setWindowSize(int windowSize) {
			if (windowSize <= 1)
				throw new NullPointerException("windowSize must be greater than 1");

			this.windowSize = windowSize;
		}

		/**
		 * Sets the windowSize to the specified value.
		 * 
		 * @param windowSize
		 *        the windowSize to set
		 */
		public SlidingWindow withWindowSize(int windowSize) {
			setWindowSize(windowSize);
			return this;
		}

		/**
		 * Returns the condition.
		 * 
		 * @return the condition
		 */
		public BooleanExpression getCondition() {
			return this.condition;
		}

		/**
		 * Sets the condition to the specified value.
		 * 
		 * @param condition
		 *        the condition to set
		 */
		public void setCondition(BooleanExpression condition) {
			if (condition == null)
				throw new NullPointerException("condition must not be null");

			this.condition = condition;
		}

		/**
		 * Sets the condition to the specified value.
		 * 
		 * @param condition
		 *        the condition to set
		 */
		public SlidingWindow withCondition(BooleanExpression condition) {
			setCondition(condition);

			return this;
		}

		public static class Implementation extends GenericSopremoReduce<IArrayNode<IJsonNode>, IJsonNode> {

			private int windowSize;

			private BooleanExpression condition;

			private FastList<IJsonNode> ringBuffer = new FastList<IJsonNode>();

			private transient IArrayNode<IJsonNode> pair = new ArrayNode<IJsonNode>();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoReduce#reduce(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(IStreamNode<IArrayNode<IJsonNode>> values, JsonCollector<IJsonNode> collector) {
				// fill buffer with remainder of previous partition
				final Iterator<IArrayNode<IJsonNode>> iterator = values.iterator();
				boolean correctPartition = false;
				while (iterator.hasNext()) {
					IArrayNode<IJsonNode> partitionedValue = iterator.next();

					if (this.ringBuffer.size() >= this.windowSize)
						this.ringBuffer.removeFirst();

					this.pair.set(1, partitionedValue.get(2));
					if (correctPartition || (correctPartition = partitionedValue.get(0).equals(partitionedValue.get(1)))) {
						for (FastList.Node<IJsonNode> n = this.ringBuffer.head(), end = this.ringBuffer.tail(); (n = n.getNext()) != end;) {
							this.pair.set(0, n.getValue());
							if (this.condition.evaluate(this.pair) == BooleanNode.TRUE)
								collector.collect(this.pair);
						}
					}

					this.ringBuffer.add(partitionedValue.get(2));
				}
			}
		}

	}

	@InputCardinality(2)
	public static class PartitionKeys extends ElementaryOperator<PartitionKeys> {
		private int windowSize;

		private int numberOfPartitions;

		/**
		 * Initializes SortedNeighborhood.PartitionKeys.
		 */
		public PartitionKeys() {
		}

		protected PartitionKeys(int windowSize, int numberOfPartitions) {
			this.windowSize = windowSize;
			this.numberOfPartitions = numberOfPartitions;
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
		 * Returns the numberOfPartitions.
		 * 
		 * @return the numberOfPartitions
		 */
		public int getNumberOfPartitions() {
			return this.numberOfPartitions;
		}

		public static class Implementation extends SopremoCoGroup {
			private int windowSize;

			private int numberOfPartitions;

			private final IntNode partitionNode = new IntNode(), originalPartitionNode = new IntNode();

			private final ArrayNode<IJsonNode> emitNode = new ArrayNode<IJsonNode>(this.partitionNode,
				this.originalPartitionNode, NullNode.getInstance());

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.type.IStreamNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamNode<IJsonNode> values1, IStreamNode<IJsonNode> values2,
					JsonCollector<IJsonNode> out) {
				@SuppressWarnings("unchecked")
				final IArrayNode<IntNode> sortingKeyRank = (IArrayNode<IntNode>) values2.iterator().next();
				final int keyRank = sortingKeyRank.get(1).getIntValue();
				final int numberOfRecords = sortingKeyRank.get(2).getIntValue();

				final Iterator<IJsonNode> iterator = values1.iterator();
				for (int index = 0; iterator.hasNext(); index++) {
					final IJsonNode record = iterator.next();
					final int partitionNumber = (keyRank + index) * this.numberOfPartitions / numberOfRecords;
					final int partitionNumberForReplication =
						(keyRank + index + this.windowSize - 1) * this.numberOfPartitions / numberOfRecords;

					if ((partitionNumber + 1 == partitionNumberForReplication) &&
						(partitionNumberForReplication < this.numberOfPartitions))
						emit(out, partitionNumberForReplication, partitionNumber, record);
					emit(out, partitionNumber, partitionNumber, record);
				}
			}

			private void emit(JsonCollector<IJsonNode> out, int partitionNumber, int originalPartitionNumber,
					IJsonNode record) {
				this.partitionNode.setValue(partitionNumber);
				this.originalPartitionNode.setValue(originalPartitionNumber);
				this.emitNode.set(2, record);
				out.collect(this.emitNode);
			}

		}
	}

	@InputCardinality(2)
	public static class SinglePass extends ElementaryDuplicateDetectionAlgorithm<SinglePass> {
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