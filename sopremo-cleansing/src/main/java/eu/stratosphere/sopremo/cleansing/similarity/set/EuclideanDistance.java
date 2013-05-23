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

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <code>EuclideanDistance</code> compares two {@link IJsonNode}s based on the Euclidean Distance attribute.
 * 
 * @author Arvid Heise
 */
public class EuclideanDistance extends SetSimilarity {
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.set.SetSimilarity#getSetSimilarity(eu.stratosphere.sopremo.type.
	 * IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double getSetSimilarity(IArrayNode<IJsonNode> node1, IArrayNode<IJsonNode> node2) {
		final Object2IntMap<IJsonNode> termFrequencies1 = this.getTermFrequencies(node1);
		final Object2IntMap<IJsonNode> termFrequencies2 = this.getTermFrequencies(node2);

		final Set<IJsonNode> values = new HashSet<IJsonNode>(termFrequencies1.keySet());
		values.addAll(termFrequencies2.keySet());

		double distanceSq = 0;
		for (IJsonNode value : values) {
			int diff = termFrequencies1.getInt(value) - termFrequencies2.getInt(value);
			distanceSq += diff * diff;
		}
		final double maxSq = node1.size() * node1.size() + node2.size() * node2.size();

		return Math.pow(distanceSq / maxSq, 0.5);
	}
}