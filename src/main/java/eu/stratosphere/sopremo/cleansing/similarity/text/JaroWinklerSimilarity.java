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

/**
 * <code>JaroWinklerSimilarity</code> compares two {@link IJsonNode}s based on the extended Jaro Distance attribute.
 * 
 * @author Arvid Heise
 */
public class JaroWinklerSimilarity extends TextSimilarity {
	private float minSimilarityForBoost = 0.7f;

	private final JaroSimilarity jaroSimilarity = new JaroSimilarity();

	/**
	 * Sets the minSimilarityForBoost to the specified value.
	 * 
	 * @param minSimilarityForBoost
	 *        the minSimilarityForBoost to set
	 */
	public void setMinSimilarityForBoost(float minSimilarityForBoost) {
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
	public float getSimilarity(CharSequence text1, CharSequence text2) {
		final float jaroSimilarity = this.jaroSimilarity.getSimilarity(text1, text2);

		if (jaroSimilarity < this.minSimilarityForBoost)
			return jaroSimilarity;

		int prefixLength = 0;
		final int maxLength = Math.min(5, this.jaroSimilarity.commonChars - this.jaroSimilarity.transpositions);
		while (prefixLength < maxLength && text1.charAt(prefixLength) == text2.charAt(prefixLength))
			prefixLength++;

		if (prefixLength == 0)
			return jaroSimilarity;

		return jaroSimilarity + 0.1f * prefixLength * (1 - jaroSimilarity);
	}
}