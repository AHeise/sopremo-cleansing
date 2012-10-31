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
package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleArrayMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class BeliefResolution extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = -295135181065628313L;

	private final EvaluationExpression[] evidences;

	/**
	 * Initializes BelieveResolution.
	 * 
	 * @param evidences
	 */
	public BeliefResolution(EvaluationExpression... evidences) {
		this.evidences = evidences;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.fusion.FusionRule#fuse(eu.stratosphere.sopremo.type.IArrayNode, double[],
	 * eu.stratosphere.sopremo.cleansing.fusion.FusionContext)
	 */
	@Override
	public void fuse(IArrayNode values, double[] weights, FusionContext context) {
		List<IJsonNode> mostProbableValues = getFinalMassFunction(values, weights, context).getMostProbableValues();
		values.clear();
		values.addAll(mostProbableValues);
	}

	protected BeliefMassFunction getFinalMassFunction(IArrayNode values, double[] weights, FusionContext context) {
		Deque<BeliefMassFunction> massFunctions = new LinkedList<BeliefMassFunction>();

		// TODO: add support for arrays
		for (int index = 0, size = values.size(); index < size; index++)
			if (!values.get(index).isNull())
				massFunctions.add(new BeliefMassFunction(values.get(index), weights[index]));

		while (massFunctions.size() > 1)
			massFunctions.addFirst(massFunctions.removeFirst().combine(massFunctions.removeFirst(), this.evidences,
				context));

		return massFunctions.getFirst();
	}

	static class BeliefMassFunction {
		private final Object2DoubleMap<IJsonNode> valueMasses = new Object2DoubleArrayMap<IJsonNode>();

		private final static IJsonNode ALL = new ArrayNode();

		/**
		 * Initializes BeliefMassFunction.
		 */
		public BeliefMassFunction(IJsonNode value, double initialMass) {
			this.valueMasses.put(value, initialMass);
			this.valueMasses.put(ALL, 1 - initialMass);
		}

		/**
		 * Initializes BeliefResolution.BeliefMassFunction.
		 */
		public BeliefMassFunction() {
		}

		/**
		 * @return
		 */
		public List<IJsonNode> getMostProbableValues() {
			double maxBelief = 0;
			List<IJsonNode> maxValues = new LinkedList<IJsonNode>();
			for (Object2DoubleMap.Entry<IJsonNode> entry : this.valueMasses.object2DoubleEntrySet()) {
				if (entry.getDoubleValue() > maxBelief) {
					maxValues.clear();
					maxValues.add(entry.getKey());
					maxBelief = entry.getDoubleValue();
				} else if (entry.getDoubleValue() == maxBelief)
					maxValues.add(entry.getKey());
			}
			return maxValues;
		}

		/**
		 * Returns the valueMasses.
		 * 
		 * @return the valueMasses
		 */
		public Object2DoubleMap<IJsonNode> getValueMasses() {
			return this.valueMasses;
		}

		/**
		 * @param removeLast
		 */
		public BeliefMassFunction combine(BeliefMassFunction other,
				EvaluationExpression[] evidenceExpressions, FusionContext context) {
			BeliefMassFunction combined = new BeliefMassFunction();

			Object2DoubleMap<IJsonNode> nominators1 = new Object2DoubleArrayMap<IJsonNode>();
			Object2DoubleMap<IJsonNode> nominators2 = new Object2DoubleArrayMap<IJsonNode>();
			// Object2DoubleMap<IJsonNode> denominators2 = new Object2DoubleArrayMap<IJsonNode>();

			double denominator = 1;

			for (Object2DoubleMap.Entry<IJsonNode> entry1 : this.valueMasses.object2DoubleEntrySet()) {
				for (Object2DoubleMap.Entry<IJsonNode> entry2 : other.valueMasses.object2DoubleEntrySet()) {
					IJsonNode value1 = entry1.getKey();
					IJsonNode value2 = entry2.getKey();
					boolean equal = value1.equals(value2);
					boolean isFirstEvidenceForSecond = equal
						|| isEvidence(value1, value2, evidenceExpressions, context);
					boolean isSecondEvidenceForFirst = equal
						|| isEvidence(value2, value1, evidenceExpressions, context);

					double mass1 = entry1.getDoubleValue();
					double mass2 = entry2.getDoubleValue();
					double massProduct = mass1 * mass2;
					if (isSecondEvidenceForFirst)
						nominators1.put(value1, nominators1.getDouble(value1) + massProduct);
					if (isFirstEvidenceForSecond)
						nominators2.put(value2, nominators2.getDouble(value2) + massProduct);
					if (!isFirstEvidenceForSecond && !isSecondEvidenceForFirst)
						denominator -= massProduct;
				}
			}

			for (Object2DoubleMap.Entry<IJsonNode> entry1 : this.valueMasses.object2DoubleEntrySet()) {
				IJsonNode value = entry1.getKey();
				combined.valueMasses.put(value, combined.valueMasses.getDouble(value) + nominators1.getDouble(value)
					/ denominator);
			}
			for (Object2DoubleMap.Entry<IJsonNode> entry2 : other.valueMasses.object2DoubleEntrySet()) {
				IJsonNode value = entry2.getKey();
				combined.valueMasses.put(value, nominators2.getDouble(value) / denominator);
			}

			return combined;
		}

		private boolean isEvidence(IJsonNode node1, IJsonNode node2, EvaluationExpression[] evidenceExpressions,
				FusionContext context) {
			if (node1 == ALL)
				return true;

			if (node2 == ALL)
				return false;

			for (int index = 0; index < evidenceExpressions.length; index++)
				if (evidenceExpressions[index].evaluate(new ArrayNode(node1, node2), null, context) == BooleanNode.TRUE)
					return true;

			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			for (Object2DoubleMap.Entry<IJsonNode> entry : this.valueMasses.object2DoubleEntrySet())
				builder.append(String.format("%s=%.2f; ", entry.getKey(), entry.getDoubleValue()));
			return builder.toString();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append(" with evidences ").append(Arrays.asList(this.evidences));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.evidences.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		BeliefResolution other = (BeliefResolution) obj;
		return Arrays.equals(this.evidences, other.evidences);
	}

}
