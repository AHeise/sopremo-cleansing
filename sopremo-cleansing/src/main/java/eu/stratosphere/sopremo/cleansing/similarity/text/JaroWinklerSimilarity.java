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

import eu.stratosphere.sopremo.EvaluationContext;

/**
 * <code>JaroWinklerSimilarity</code> compares two {@link IJsonNode}s based on the extended Jaro Distance attribute.
 * 
 * @author Arvid Heise
 */
public class JaroWinklerSimilarity extends JaroSimilarity {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8293892965742742690L;

	private double minSimilarityForBoost = 0.7;

	/**
	 * Sets the minSimilarityForBoost to the specified value.
	 * 
	 * @param minSimilarityForBoost
	 *        the minSimilarityForBoost to set
	 */
	public void setMinSimilarityForBoost(double minSimilarityForBoost) {
		if (minSimilarityForBoost < 0 || minSimilarityForBoost > 1)
			throw new IllegalArgumentException("minSimilarityForBoost must be in [0; 1]");

		this.minSimilarityForBoost = minSimilarityForBoost;
	}

	/**
	 * Returns the minSimilarityForBoost.
	 * 
	 * @return the minSimilarityForBoost
	 */
	public double getMinSimilarityForBoost() {
		return this.minSimilarityForBoost;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.JaroSimilarity#getSimilarity(java.lang.CharSequence,
	 * java.lang.CharSequence, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(CharSequence text1, CharSequence text2, EvaluationContext context) {
		final double jaroSimilarity = super.getSimilarity(text1, text2, context);

		if (jaroSimilarity < this.minSimilarityForBoost)
			return jaroSimilarity;

		int prefixLength = 0;
		final int maxLength = this.commonChars - this.transpositions;
		while (prefixLength < maxLength && text1.charAt(prefixLength) == text2.charAt(prefixLength))
			prefixLength++;

		if (prefixLength == 0)
			return jaroSimilarity;

		return jaroSimilarity + 0.1 * prefixLength * (1 - jaroSimilarity);
	}
}