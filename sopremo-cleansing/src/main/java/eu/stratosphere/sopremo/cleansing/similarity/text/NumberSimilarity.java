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

import java.io.IOException;

import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.type.INumericNode;

/**
 * This {@link Similarity} implementation checks the absolute variation of the numbers of two {@link IJsonNode}s.
 * The maximum allowed variation is defined by an absolute value.
 * 
 * @author David Sonnabend
 * @author Arvid Heise
 */
public class NumberSimilarity extends AbstractSimilarity<INumericNode> {
	/**
	 * The absolute value of maximum allowed variation.
	 */
	private double maxDiff = 1;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#compareNodes(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public double getSimilarity(INumericNode node1, INumericNode node2) {
		final double diff = Math.abs(node1.getDoubleValue() - node2.getDoubleValue());
		if (diff == 0)
			return 1;
		if (diff > this.maxDiff)
			return 0;
		return 1 - diff / this.maxDiff;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<INumericNode> getExpectedType() {
		return INumericNode.class;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		NumberSimilarity other = (NumberSimilarity) obj;
		return Double.doubleToLongBits(this.maxDiff) == Double.doubleToLongBits(other.maxDiff);
	}

	public double getMaxDiff() {
		return this.maxDiff;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		long temp;
		temp = Double.doubleToLongBits(this.maxDiff);
		result = prime * result + (int) (temp ^ temp >>> 32);
		return result;
	}

	public void setMaxDiff(double maxDiff) {
		if (maxDiff <= 0)
			throw new IllegalArgumentException("maxDiff must be positive");

		this.maxDiff = maxDiff;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		appendable.append("[maxDiff=");
		TypeFormat.format(this.maxDiff, appendable);
		appendable.append("]");
	}

	public NumberSimilarity withMaxDiff(double maxDiff) {
		this.setMaxDiff(maxDiff);
		return this;
	}

}
