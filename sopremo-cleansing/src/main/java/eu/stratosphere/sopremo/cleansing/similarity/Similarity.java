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

package eu.stratosphere.sopremo.cleansing.similarity;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * The <code>interface</code> <code>Similarity</code> provides methods
 * for getting a similarity measure of two {@link IJsonNode} instances.
 * 
 * @author Matthias Pohl
 * @author Arvid Heise
 * @see AbstractSimilarity
 */
public interface Similarity<NodeType extends IJsonNode> extends ISerializableSopremoType {
	public static double NOT_APPLICABLE = Double.valueOf("-0");

	/**
	 * Compares two nodes and returns a similarity value that should be in the range [0;1].
	 * 
	 * @param node1
	 *        The first object.
	 * @param node2
	 *        The second object.
	 * @return A value between <code>0.0</code> (not equal) and <code>1.0</code> (the same object).
	 */
	public double getSimilarity(NodeType node1, NodeType node2);

	public Class<NodeType> getExpectedType();
	
	public boolean isSymmetric();
	
	public Similarity<NodeType> clone();
}