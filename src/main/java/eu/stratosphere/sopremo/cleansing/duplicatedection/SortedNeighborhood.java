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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javolution.text.TypeFormat;
import javolution.util.FastList;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.*;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.pact.GenericSopremoCoGroup;
import eu.stratosphere.sopremo.pact.GenericSopremoReduce;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.*;

/**
 * @author Arvid Heise
 */
@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = { "snm", "sorted neighborhood" })
public class SortedNeighborhood extends CompositeDuplicateDetectionAlgorithm<SortedNeighborhood> {
	private static final int DEFAULT_WINDOW_SIZE = 2;

	private int windowSize = DEFAULT_WINDOW_SIZE;

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
	@Property
	@Name(preposition = { "with window", "with window size" })
	public void setWindowSize(int windowSize) {
		if (windowSize < 2)
			throw new IllegalArgumentException("window size must be >= 2");
		this.windowSize = windowSize;
	}

	public SortedNeighborhood withWindowSize(int windowSize) {
		setWindowSize(windowSize);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.windowSize;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortedNeighborhood other = (SortedNeighborhood) obj;
		return this.windowSize == other.windowSize;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		appendable.append(", window size: ");
		TypeFormat.format(this.windowSize, appendable);
	}

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
			int oldRank = this.rank;
			this.rank += rankNode.getIntValue();
			rankNode.setValue(oldRank);
			return node;
		}
	}

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
			PairFilter pairFilter, CandidateComparison comparison) {
		final Grouping countRecords = new Grouping().
			withInputs(inputs).
			withResultProjection(CoreFunctions.COUNT.inline(new InputSelection(0)));
		List<JsonStream> passResults = new ArrayList<JsonStream>();
		for (Pass pass : selection.getPasses()) {
			final EvaluationExpression blockingKey = pass.getBlockingKey(0);
			// record -> sorting key
			final Projection sortingKeys = new Projection().
				withInputs(inputs.get(0)).
				withResultProjection(blockingKey);

			// [sorting key]+ -> [sorting key, count]
			final BatchAggregationExpression countExpression = new BatchAggregationExpression();
			final Grouping sortingKeyCount = new Grouping().
				withInputs(sortingKeys).
				withGroupingKey(EvaluationExpression.VALUE).
				withResultProjection(
					new ArrayCreation().
						add(countExpression.add(CoreFunctions.FIRST)).
						add(countExpression.add(CoreFunctions.COUNT))).
				withName("Generate KPM List " + blockingKey);

			// [sorting key, count] -> [sorting key, rank]
			final Sort keysWithRank = new Sort().
				withInputs(sortingKeyCount).
				withResultProjection(new EnumerationExpression());

			// [sorting key, rank] -> [sorting key, rank, count]
			final ContextualProjection keysWithRankAndCount = new ContextualProjection().
				withInputs(keysWithRank, countRecords).
				withContextPath(new ArrayAccess(2));

			if (inputs.size() == 1) {
				// record + [sorting key, rank, count] -> [partition, partition, record]+
				final PartitionKeys rankJoin = new PartitionKeys(this.windowSize, getDegreeOfParallelism()).
					withInputs(inputs.get(0), keysWithRankAndCount).
					withKeyExpression(0, blockingKey).
					withKeyExpression(1, new ArrayAccess(0));

				final SingleSourceSlidingWindow window = new SingleSourceSlidingWindow().
					withInputs(rankJoin).
					withWindowSize(this.windowSize).
					withCandidateComparison(comparison).
					withKeyExpression(0, new ArrayAccess(0)).
					withInnerGroupOrdering(0, new OrderingExpression(Order.ASCENDING,
						new ChainedSegmentExpression(new ArrayAccess(2), pass.getBlockingKeys().get(0)))).
					withResultProjection(comparison.getResultProjection());
				passResults.add(window);
			} else {
				// record + [sorting key, rank, count] -> [key, partition, record]
				final PartitionKeys rankJoin1 = new LeftPartitionKeys(this.windowSize, getDegreeOfParallelism()).
					withInputs(inputs.get(0), keysWithRankAndCount).
					withKeyExpression(0, blockingKey).
					withKeyExpression(1, new ArrayAccess(0));

				final EvaluationExpression sortingKey2 = pass.getBlockingKey(1);
				// record -> sorting key
				final Projection sortingKeys2 = new Projection().
					withInputs(inputs.get(1)).
					withResultProjection(sortingKey2);
				
				// [sorting key]+ -> [sorting key, count]
				final Grouping sortingKeyCount2 = new Grouping().
					withInputs(sortingKeys2).
					withGroupingKey(EvaluationExpression.VALUE).
					withResultProjection(
						new ArrayCreation().
							add(countExpression.add(CoreFunctions.FIRST)).
							add(countExpression.add(CoreFunctions.COUNT))).
					withName("Generate KPM List " + sortingKey2);

				// [sorting key, rank, count]? + [sorting key, count]* -> [sorting key, partition, [window borders]*]
				final CoPartition keys2WithPartition = new CoPartition(this.windowSize, getDegreeOfParallelism()).
					withInputs(keysWithRankAndCount, sortingKeyCount2);
				
				// record + [sorting key, partition, [window borders]*] -> [partition, partition, record]+
				final PartitionKeys rankJoin2 = new RightPartitionKeys(this.windowSize, getDegreeOfParallelism()).
					withInputs(inputs.get(0), keys2WithPartition).
					withKeyExpression(0, sortingKey2).
					withKeyExpression(1, new ArrayAccess(0));

				final DualSourceSlidingWindow window = new DualSourceSlidingWindow().
					withInputs(rankJoin1, rankJoin2).
					withWindowSize(this.windowSize).
					withCandidateComparison(comparison).
					withSortingKey2(sortingKey2).
					withKeyExpression(0, new ArrayAccess(0)).
					withKeyExpression(1, new ArrayAccess(0)).
					withInnerGroupOrdering(0, new OrderingExpression(Order.ASCENDING,
						new ChainedSegmentExpression(new ArrayAccess(2), pass.getBlockingKeys().get(0)))).
					withInnerGroupOrdering(1, new OrderingExpression(Order.ASCENDING,
						new ChainedSegmentExpression(new ArrayAccess(2), pass.getBlockingKeys().get(0)))).
					withResultProjection(comparison.getResultProjection());
				passResults.add(window);
			}
		}
		return new UnionAll().withInputs(passResults);
	}

	// [sorting key, rank, count]? + [sorting key, count]* -> [sorting key, partition, [window borders]*]
	@InputCardinality(2)
	@DegreeOfParallelism(1)
	public static class CoPartition extends ElementaryOperator<CoPartition> {
		/**
		 * Initializes Sort.
		 */
		public CoPartition() {
			this.setKeyExpressions(0, ConstantExpression.NULL);
			this.setInnerGroupOrder(0, new OrderingExpression(Order.DESCENDING, EvaluationExpression.VALUE));
			this.setKeyExpressions(1, ConstantExpression.NULL);
			this.setInnerGroupOrder(1, new OrderingExpression(Order.DESCENDING, EvaluationExpression.VALUE));
		}

		private int windowSize;

		private int numberOfPartitions;

		protected CoPartition(int windowSize, int numberOfPartitions) {
			this();
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

		public static class Implementation extends
				GenericSopremoCoGroup<IArrayNode<IJsonNode>, IArrayNode<IJsonNode>, IJsonNode> {
			private int partitionNumber;

			private int windowSize;

			private int numberOfPartitions;

			private CachingArrayNode<IntNode> replicationIndices = new CachingArrayNode<IntNode>();

			private IntList replicationsLeft = new IntArrayList();

			private IntNode partitionNode = new IntNode();

			private IArrayNode<IJsonNode> emitNode = new ArrayNode<IJsonNode>(NullNode.getInstance(),
				this.partitionNode, this.replicationIndices);

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#open(eu.stratosphere.configuration.Configuration)
			 */
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				this.partitionNumber = this.numberOfPartitions - 1;
				this.partitionNode.setValue(this.partitionNumber);
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.type.IStreamNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamNode<IArrayNode<IJsonNode>> values1,
					IStreamNode<IArrayNode<IJsonNode>> values2,
					JsonCollector<IJsonNode> out) {

				final Iterator<IArrayNode<IJsonNode>> iterator1 = values1.iterator();
				final Iterator<IArrayNode<IJsonNode>> iterator2 = values2.iterator();

				IArrayNode<IJsonNode> sortingKeyRank = null, keyWithCount = null;
				boolean hasNext1 = iterator1.hasNext(), hasNext2 = iterator2.hasNext();
				while (hasNext1 || hasNext2) {
					if (sortingKeyRank == null && hasNext1)
						sortingKeyRank = iterator1.next();
					if (keyWithCount == null && hasNext2)
						keyWithCount = iterator2.next();

					int comparison = hasNext1 ? sortingKeyRank.get(0).compareTo(keyWithCount.get(0)) : -1;
					if (comparison >= 0) {
						final int keyRank = ((IntNode) sortingKeyRank.get(1)).getIntValue();
						final int numberOfRecords = ((IntNode) sortingKeyRank.get(2)).getIntValue();
						final int newPartitionNumber = keyRank * this.numberOfPartitions / numberOfRecords;
						for (int p = this.partitionNumber; p > newPartitionNumber; p--) {
							this.replicationsLeft.add(0, this.windowSize);
							final IntNode unusedNode = this.replicationIndices.getUnusedNode();
							this.replicationIndices.add(unusedNode == null ? new IntNode() : unusedNode);
						}
						this.partitionNumber = newPartitionNumber;
						this.partitionNode.setValue(this.partitionNumber);
						sortingKeyRank = null;
						hasNext1 = iterator1.hasNext();
					}
					if (comparison <= 0) {
						final int count = ((IntNode) keyWithCount.get(1)).getIntValue();

						for (int p = this.replicationsLeft.size() - 1; p >= 0; p--) {
							final IntNode index = this.replicationIndices.get(p);
							int remaining = this.replicationsLeft.getInt(p);
							index.setValue(Math.max(0, count - remaining));
							this.replicationsLeft.set(p, remaining - count);
						}

						this.emitNode.set(0, keyWithCount.get(0));
						out.collect(this.emitNode);
						keyWithCount = null;
						hasNext2 = iterator2.hasNext();

						for (int p = this.replicationsLeft.size() - 1; p >= 0; p--)
							if (replicationsLeft.getInt(p) <= 0) {
								this.replicationIndices.remove(p);
								this.replicationsLeft.remove(p);
							}
					}

				}
			}
		}
	}

	@InputCardinality(1)
	public static class SingleSourceSlidingWindow extends
			ElementaryDuplicateDetectionAlgorithm<SingleSourceSlidingWindow> {
		private int bufferSize = DEFAULT_WINDOW_SIZE - 1;

		/**
		 * Initializes SortedNeighborhood.SingleSourceSlidingWindow.
		 */
		public SingleSourceSlidingWindow() {
		}

		/**
		 * Returns the windowSize.
		 * 
		 * @return the windowSize
		 */
		public int getWindowSize() {
			return this.bufferSize + 1;
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

			this.bufferSize = windowSize - 1;
		}

		/**
		 * Sets the windowSize to the specified value.
		 * 
		 * @param windowSize
		 *        the windowSize to set
		 */
		public SingleSourceSlidingWindow withWindowSize(int windowSize) {
			setWindowSize(windowSize);
			return this;
		}

		public static class Implementation extends GenericSopremoReduce<IArrayNode<IJsonNode>, IJsonNode> {

			private int bufferSize;

			private CandidateComparison candidateComparison;

			private FastList<IJsonNode> ringBuffer = new FastList<IJsonNode>();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoReduce#reduce(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(IStreamNode<IArrayNode<IJsonNode>> values, JsonCollector<IJsonNode> collector) {
				final Iterator<IArrayNode<IJsonNode>> iterator = values.iterator();
				boolean correctPartition = false;
				while (iterator.hasNext()) {
					IArrayNode<IJsonNode> partitionedValue = iterator.next();

					final IJsonNode value = partitionedValue.get(2);
					if (correctPartition ||
						(correctPartition = partitionedValue.get(0).equals(partitionedValue.get(1)))) {
						for (FastList.Node<IJsonNode> n = this.ringBuffer.head(), end = this.ringBuffer.tail(); (n =
							n.getNext()) != end;)
							this.candidateComparison.performComparison(n.getValue(), value, collector);
					}

					if (this.ringBuffer.size() >= this.bufferSize)
						this.ringBuffer.removeFirst();
					this.ringBuffer.add(value.clone());
				}

				if (this.ringBuffer.size() >= this.bufferSize)
					this.ringBuffer.removeFirst();
				while (!this.ringBuffer.isEmpty()) {
					final IJsonNode value = this.ringBuffer.removeFirst();
					for (FastList.Node<IJsonNode> n = this.ringBuffer.head(), end = this.ringBuffer.tail(); (n =
						n.getNext()) != end;)
						this.candidateComparison.performComparison(value, n.getValue(), collector);
				}
			}
		}
	}

	@InputCardinality(2)
	public static class DualSourceSlidingWindow extends ElementaryDuplicateDetectionAlgorithm<DualSourceSlidingWindow> {
		private int bufferSize = DEFAULT_WINDOW_SIZE - 1;

		private EvaluationExpression sortingKey2;

		/**
		 * Initializes SortedNeighborhood.SingleSourceSlidingWindow.
		 */
		public DualSourceSlidingWindow() {
		}

		/**
		 * Returns the windowSize.
		 * 
		 * @return the windowSize
		 */
		public int getWindowSize() {
			return this.bufferSize + 1;
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

			this.bufferSize = windowSize - 1;
		}

		/**
		 * Sets the windowSize to the specified value.
		 * 
		 * @param windowSize
		 *        the windowSize to set
		 */
		public DualSourceSlidingWindow withWindowSize(int windowSize) {
			setWindowSize(windowSize);
			return this;
		}

		/**
		 * Sets the sortingKey2 to the specified value.
		 * 
		 * @param sortingKey
		 *        the sortingKey2 to set
		 */
		public void setSortingKey2(EvaluationExpression sortingKey) {
			if (sortingKey == null)
				throw new NullPointerException("sortingKey2 must not be null");

			this.sortingKey2 = sortingKey;
		}

		/**
		 * Sets the sortingKey2 to the specified value.
		 * 
		 * @param sortingKey
		 *        the sortingKey2 to set
		 * @return this
		 */
		public DualSourceSlidingWindow withSortingKey2(EvaluationExpression sortingKey) {
			setSortingKey2(sortingKey);
			return this;
		}

		/**
		 * Returns the sortingKey2.
		 * 
		 * @return the sortingKey2
		 */
		public EvaluationExpression getSortingKey2() {
			return this.sortingKey2;
		}

		private static class KeyedValue {
			final IJsonNode key, value;

			public KeyedValue(IJsonNode key, IJsonNode value) {
				this.key = key;
				this.value = value;
			}
		}

		public static class Implementation extends
				GenericSopremoCoGroup<IArrayNode<IJsonNode>, IArrayNode<IJsonNode>, IJsonNode> {

			private int bufferSize;

			private CandidateComparison candidateComparison;

			private FastList<KeyedValue> smallerBuffer = new FastList<KeyedValue>(),
					largerBuffer = new FastList<KeyedValue>();

			private EvaluationExpression sortingKey2;

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.type.IStreamNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamNode<IArrayNode<IJsonNode>> values1,
					IStreamNode<IArrayNode<IJsonNode>> values2, JsonCollector<IJsonNode> collector) {
				final Iterator<IArrayNode<IJsonNode>> iterator1 = values1.iterator();
				final Iterator<IArrayNode<IJsonNode>> iterator2 = values2.iterator();

				// fill ring buffer with overlapping values from the previous partition
				boolean correctPartition = false;
				while (iterator2.hasNext()) {
					IArrayNode<IJsonNode> partitionedValue = iterator2.next();
					if (correctPartition ||
						(correctPartition = partitionedValue.get(0).equals(partitionedValue.get(1)))) {
						if (this.largerBuffer.size() >= this.bufferSize)
							this.smallerBuffer.add(this.largerBuffer.removeFirst());
						this.largerBuffer.add(toKeyedValue(partitionedValue));
						break;
					}
					else if (this.largerBuffer.size() >= this.bufferSize)
						this.largerBuffer.removeFirst();
					this.largerBuffer.add(toKeyedValue(partitionedValue));
				}

				while (iterator1.hasNext()) {
					final IArrayNode<IJsonNode> pivot = iterator1.next();
					final IJsonNode key = pivot.get(2);
					final IJsonNode value = pivot.get(3);

					// move window in second source so that pivot would be in the middle
					while (!this.largerBuffer.isEmpty() &&
						this.largerBuffer.head().getValue().key.compareTo(key) < 0) {
						if (this.smallerBuffer.size() >= this.bufferSize)
							this.smallerBuffer.removeFirst();
						this.smallerBuffer.add(this.largerBuffer.removeFirst());
					}

					while (this.largerBuffer.size() < this.bufferSize && iterator2.hasNext())
						this.largerBuffer.add(toKeyedValue(iterator2.next()));

					// now compare with the buffers
					for (FastList.Node<KeyedValue> n = this.smallerBuffer.head(), end =
						this.smallerBuffer.tail(); (n =
						n.getNext()) != end;)
						this.candidateComparison.performComparison(n.getValue().value, value, collector);
					for (FastList.Node<KeyedValue> n = this.largerBuffer.head(), end =
						this.largerBuffer.tail(); (n =
						n.getNext()) != end;)
						this.candidateComparison.performComparison(n.getValue().value, value, collector);
				}
			}

			private KeyedValue toKeyedValue(IArrayNode<IJsonNode> partitionedValue) {
				final IJsonNode value = partitionedValue.get(3).clone();
				return new KeyedValue(this.sortingKey2.evaluate(value), value);
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
			 * @see
			 * eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#open(eu.stratosphere.nephele.configuration.Configuration
			 * )
			 */
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				if (this.numberOfPartitions == STANDARD_DEGREE_OF_PARALLELISM)
					this.numberOfPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
			}

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
					final int partitionNumberForReplication = Math.min(this.numberOfPartitions - 1,
						(keyRank + index + this.windowSize - 1) * this.numberOfPartitions / numberOfRecords);

					for (int replicationNumber = partitionNumber; replicationNumber <= partitionNumberForReplication; replicationNumber++)
						emit(out, replicationNumber, partitionNumber, record);
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

	// record + [sorting key, partition, [window borders]*] -> [partition, partition, record]+
	@InputCardinality(2)
	public static class RightPartitionKeys extends PartitionKeys {

		public RightPartitionKeys() {
			super();
		}

		public RightPartitionKeys(int windowSize, int numberOfPartitions) {
			super(windowSize, numberOfPartitions);
		}

		public static class Implementation extends SopremoCoGroup {
			private int windowSize;

			private int numberOfPartitions;

			private final IntNode partitionNode = new IntNode(), originalPartitionNode = new IntNode();

			private final ArrayNode<IJsonNode> emitNode = new ArrayNode<IJsonNode>(this.partitionNode,
				this.originalPartitionNode, NullNode.getInstance());

			/*
			 * (non-Javadoc)
			 * @see
			 * eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#open(eu.stratosphere.nephele.configuration.Configuration
			 * )
			 */
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				if (this.numberOfPartitions == STANDARD_DEGREE_OF_PARALLELISM)
					this.numberOfPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.type.IStreamNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamNode<IJsonNode> values1, IStreamNode<IJsonNode> values2,
					JsonCollector<IJsonNode> out) {
				@SuppressWarnings("unchecked")
				final IArrayNode<IJsonNode> sortingKeyRank = (IArrayNode<IJsonNode>) values2.iterator().next();
				final int partitionNumber = ((IntNode) sortingKeyRank.get(1)).getIntValue();
				@SuppressWarnings("unchecked")
				final IArrayNode<IntNode> replicationIndices = (IArrayNode<IntNode>) sortingKeyRank.get(2);

				final Iterator<IJsonNode> iterator = values1.iterator();
				int partitionNumberForReplication = partitionNumber, replicationIndicesIndex = 0;
				for (int index = 0; iterator.hasNext(); index++) {
					final IJsonNode record = iterator.next();

					for (; replicationIndicesIndex < replicationIndices.size(); partitionNumberForReplication++)
						if (index < replicationIndices.get(replicationIndicesIndex).getIntValue())
							break;

					for (int replicationNumber = partitionNumber; replicationNumber <= partitionNumberForReplication; replicationNumber++)
						emit(out, replicationNumber, partitionNumber, record);
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
	public static class LeftPartitionKeys extends PartitionKeys {

		public LeftPartitionKeys() {
			super();
		}

		public LeftPartitionKeys(int windowSize, int numberOfPartitions) {
			super(windowSize, numberOfPartitions);
		}

		public static class Implementation extends SopremoCoGroup {
			private int numberOfPartitions;

			private final IntNode partitionNode = new IntNode();

			private final ArrayNode<IJsonNode> emitNode = new ArrayNode<IJsonNode>(NullNode.getInstance(),
				this.partitionNode, NullNode.getInstance());

			/*
			 * (non-Javadoc)
			 * @see
			 * eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#open(eu.stratosphere.nephele.configuration.Configuration
			 * )
			 */
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				if (this.numberOfPartitions == STANDARD_DEGREE_OF_PARALLELISM)
					this.numberOfPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IStreamNode,
			 * eu.stratosphere.sopremo.type.IStreamNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IStreamNode<IJsonNode> values1, IStreamNode<IJsonNode> values2,
					JsonCollector<IJsonNode> out) {
				@SuppressWarnings("unchecked")
				final IArrayNode<IJsonNode> sortingKeyRank = (IArrayNode<IJsonNode>) values2.iterator().next();
				final IJsonNode key = sortingKeyRank.get(0);
				final int keyRank = ((IntNode) sortingKeyRank.get(1)).getIntValue();
				final int numberOfRecords = ((IntNode) sortingKeyRank.get(2)).getIntValue();

				final Iterator<IJsonNode> iterator = values1.iterator();
				for (int index = 0; iterator.hasNext(); index++) {
					final IJsonNode record = iterator.next();
					final int partitionNumber = (keyRank + index) * this.numberOfPartitions / numberOfRecords;
					emit(out, partitionNumber, key, record);
				}
			}

			private void emit(JsonCollector<IJsonNode> out, int partitionNumber, IJsonNode key,
					IJsonNode record) {
				this.partitionNode.setValue(partitionNumber);
				this.emitNode.set(0, key);
				this.emitNode.set(2, record);
				out.collect(this.emitNode);
			}
		}
	}

}

//
// @InputCardinality(1)
// public static class SingleSourceSlidingWindow extends
// ElementaryDuplicateDetectionAlgorithm<SingleSourceSlidingWindow> {
// private int bufferSize = DEFAULT_WINDOW_SIZE - 1;
//
// private boolean replicateBorder;
//
// /**
// * Initializes SortedNeighborhood.SingleSourceSlidingWindow.
// */
// public SingleSourceSlidingWindow() {
// }
//
// /**
// * Returns the windowSize.
// *
// * @return the windowSize
// */
// public int getWindowSize() {
// return this.bufferSize + 1;
// }
//
// /**
// * Sets the windowSize to the specified value.
// *
// * @param windowSize
// * the windowSize to set
// */
// public void setWindowSize(int windowSize) {
// if (windowSize <= 1)
// throw new NullPointerException("windowSize must be greater than 1");
//
// this.bufferSize = windowSize - 1;
// }
//
// /**
// * Sets the windowSize to the specified value.
// *
// * @param windowSize
// * the windowSize to set
// */
// public SingleSourceSlidingWindow withWindowSize(int windowSize) {
// setWindowSize(windowSize);
// return this;
// }
//
// /**
// * Sets the replicateBorder to the specified value.
// *
// * @param replicateBorder
// * the replicateBorder to set
// */
// public void setReplicateBorder(boolean replicateBorder) {
// this.replicateBorder = replicateBorder;
// }
//
// /**
// * Sets the replicateBorder to the specified value.
// *
// * @param replicateBorder
// * the replicateBorder to set
// */
// public SingleSourceSlidingWindow withReplicateBorder(boolean replicateBorder) {
// setReplicateBorder(replicateBorder);
// return this;
// }
//
// /**
// * Returns the replicateBorder.
// *
// * @return the replicateBorder
// */
// public boolean isReplicateBorder() {
// return this.replicateBorder;
// }
//
// public static class Implementation extends SopremoReduce {
//
// private int windowSize;
//
// private CandidateComparison candidateComparison = new CandidateComparison();
//
// private transient FastList<IJsonNode> smallerBuffer = new FastList<IJsonNode>();
//
// private boolean replicateBorder;
//
// private transient IArrayNode<IJsonNode> replicationArray;
//
// /*
// * (non-Javadoc)
// * @see
// * eu.stratosphere.sopremo.pact.GenericSopremoReduce#open(eu.stratosphere.nephele.configuration.Configuration
// * )
// */
// @Override
// public void open(Configuration parameters) {
// super.open(parameters);
// if (this.replicateBorder)
// this.replicationArray = new ArrayNode<IJsonNode>(3);
// }
//
// /*
// * (non-Javadoc)
// * @see eu.stratosphere.sopremo.pact.GenericSopremoReduce#reduce(eu.stratosphere.sopremo.type.IStreamNode,
// * eu.stratosphere.sopremo.pact.JsonCollector)
// */
// @Override
// protected void reduce(IStreamNode<IJsonNode> values, JsonCollector<IJsonNode> collector) {
// // fill buffer with remainder of previous partition
// final Iterator<IJsonNode> iterator = values.iterator();
//
// // fill ring buffer and replicate
// if (this.replicateBorder)
// this.replicationArray.set(1, IntNode.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
// while (this.ringBuffer.size() < this.windowSize && iterator.hasNext()) {
// final IJsonNode value = iterator.next();
// this.ringBuffer.add(value);
// if (this.replicateBorder) {
// this.replicationArray.set(2, value);
// collector.collect(this.replicationArray);
// }
// }
//
// // perform all comparisons with a filled window
// while (iterator.hasNext()) {
// final IJsonNode value = iterator.next();
// for (FastList.Node<IJsonNode> n = this.ringBuffer.head(), end = this.ringBuffer.tail(); (n =
// n.getNext()) != end;) {
// this.candidateComparison.performComparison(n.getValue(), value, collector);
// }
// this.ringBuffer.removeFirst();
// this.ringBuffer.add(value);
// }
//
// // perform the remaining comparisons and replicate
// if (this.replicateBorder)
// this.replicationArray.set(1, IntNode.valueOf(1 + getRuntimeContext().getIndexOfThisSubtask()));
// while (!this.ringBuffer.isEmpty()) {
// final IJsonNode value = this.ringBuffer.removeLast();
// for (FastList.Node<IJsonNode> n = this.ringBuffer.head(), end = this.ringBuffer.tail(); (n =
// n.getNext()) != end;) {
// this.candidateComparison.performComparison(n.getValue(), value, collector);
// }
// if (this.replicateBorder) {
// this.replicationArray.set(2, value);
// collector.collect(this.replicationArray);
// }
// }
// }
// }
// }