/*
 * DuDe - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH,
 *                     Potsdam, Germany 
 *
 * This file is part of DuDe.
 * 
 * DuDe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * DuDe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DuDe.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package eu.stratosphere.sopremo.cleansing.similarity.set;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <code>EuclideanSimilarity</code> compares two {@link IJsonNode}s based on the Euclidean Distance attribute.
 * 
 * @author Arvid Heise
 */
public class EuclideanSimilarity extends SetSimilarity {
	private final Object2IntMap<IJsonNode> termFrequencies1 = new Object2IntOpenHashMap<IJsonNode>(),
			termFrequencies2 = new Object2IntOpenHashMap<IJsonNode>();

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.set.SetSimilarity#getSetSimilarity(eu.stratosphere.sopremo.type.
	 * IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected float getSetSimilarity(IArrayNode<IJsonNode> node1, IArrayNode<IJsonNode> node2) {
		this.getTermFrequencies(node1, this.termFrequencies1);
		this.getTermFrequencies(node2, this.termFrequencies2);

		final Set<IJsonNode> values = new HashSet<IJsonNode>(this.termFrequencies1.keySet());
		values.addAll(this.termFrequencies2.keySet());

		float distanceSq = 0;
		for (IJsonNode value : values) {
			int diff = this.termFrequencies1.getInt(value) - this.termFrequencies2.getInt(value);
			distanceSq += diff * diff;
		}
		final float maxSq = node1.size() * node1.size() + node2.size() * node2.size();

		return (float) (1 - Math.pow(distanceSq / maxSq, 0.5));
	}
}