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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javolution.text.TypeFormat;
import javolution.util.FastList;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.ContextualProjection;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Sort;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.OrderingExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.GenericSopremoReduce;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.ArrayNode;
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
	
	/* (non-Javadoc)
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
		if (inputs.size() > 1)
			throw new UnsupportedOperationException();

		final Grouping countRecords = new Grouping().
			withInputs(inputs).
			withResultProjection(CoreFunctions.COUNT.inline(new InputSelection(0)));
		List<JsonStream> passResults = new ArrayList<JsonStream>();
		for (Pass pass : selection.getPasses()) {
			// record -> sorting key
			final Projection sortingKeys = new Projection().
				withInputs(inputs.get(0)).
				withResultProjection(pass.getBlockingKeys().get(0));

			// [sorting key]+ -> [sorting key, count]
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

			// [sorting key, rank] -> [sorting key, rank, count]
			final ContextualProjection keysWithRankAndCount = new ContextualProjection().
				withInputs(keysWithRank, countRecords).
				withContextPath(new ArrayAccess(2));

			// record + [sorting key, rank, count] -> [partition, partition, record]{1,3}
			final PartitionKeys rankJoin = new PartitionKeys(this.windowSize, getDegreeOfParallelism()).
				withInputs(inputs.get(0), keysWithRankAndCount).
				withKeyExpression(0, pass.getBlockingKeys().get(0)).
				withKeyExpression(1, new ArrayAccess(0));

			final SlidingWindow window = new SlidingWindow().
				withInputs(rankJoin).
				withWindowSize(this.windowSize).
				withCandidateComparison(comparison).
				withKeyExpression(0, new ArrayAccess(0)).
				withInnerGroupOrdering(0, new OrderingExpression(Order.ASCENDING,
					new ChainedSegmentExpression(new ArrayAccess(2), pass.getBlockingKeys().get(0))));
			passResults.add(window);
		}
		return new UnionAll().withInputs(passResults);
	}

	@InputCardinality(1)
	public static class SlidingWindow extends ElementaryDuplicateDetectionAlgorithm<SlidingWindow> {
		private int bufferSize = DEFAULT_WINDOW_SIZE - 1;

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
		public SlidingWindow withWindowSize(int windowSize) {
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
					this.ringBuffer.add(value);
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

}

//
// @InputCardinality(1)
// public static class SlidingWindow extends ElementaryDuplicateDetectionAlgorithm<SlidingWindow> {
// private int bufferSize = DEFAULT_WINDOW_SIZE - 1;
//
// private boolean replicateBorder;
//
// /**
// * Initializes SortedNeighborhood.SlidingWindow.
// */
// public SlidingWindow() {
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
// public SlidingWindow withWindowSize(int windowSize) {
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
// public SlidingWindow withReplicateBorder(boolean replicateBorder) {
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
// private transient FastList<IJsonNode> ringBuffer = new FastList<IJsonNode>();
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