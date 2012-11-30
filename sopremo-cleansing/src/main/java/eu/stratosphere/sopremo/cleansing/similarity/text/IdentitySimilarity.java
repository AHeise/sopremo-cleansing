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

package eu.stratosphere.sopremo.cleansing.similarity.text;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This {@link Similarity} implementation checks the identity of two attribute values.
 * 
 * @author Arvid Heise
 */
public class IdentitySimilarity extends AbstractSimilarity<IJsonNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 66432635725114156L;

	@Override
	public double getSimilarity(IJsonNode node1, IJsonNode node2) {
		return node1.equals(node2) ? 1 : 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<IJsonNode> getExpectedType() {
		return IJsonNode.class;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new IdentitySimilarity();
	}
}
