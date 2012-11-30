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

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.type.IArrayNode;

/**
 * <code>CosineSimilarity</code> compares two {@link IJsonNode}s based on the Cosine Similarity
 * attribute.
 * 
 * @author Arvid Heise
 */
public class CosineSimilarity extends SetSimilarity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3231226795346697233L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.set.SetSimilarity#getSetSimilarity(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double getSetSimilarity(IArrayNode node1, IArrayNode node2) {
		return getNumberOfCommonTokens(node1, node2) / Math.pow(node1.size(), 0.5) / Math.pow(node2.size(), 0.5);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new CosineSimilarity();
	}
}
