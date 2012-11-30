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
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <code>DiceCoefficientSimilarity</code> compares two {@link IJsonNode}s based on the Dice's Coefficient attribute.
 * 
 * @author Arvid Heise
 */
public class DiceCoefficientSimilarity extends SetSimilarity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8580009440150845162L;

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.set.SetSimilarity#getSetSimilarity(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double getSetSimilarity(IArrayNode node1, IArrayNode node2) {
		return 2 * getNumberOfCommonTokens(node1, node2) / (node1.size() + node2.size());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new DiceCoefficientSimilarity();
	}
}