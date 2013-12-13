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
 * <code>HammingSimilarity</code> compares two {@link IJsonNode}s based on the Hamming Distance.
 * 
 * @author Arvid Heise
 */
public class HammingSimilarity extends TextSimilarity {
	/**
	 * Initializes a <code>HammingSimilarity</code>.
	 */
	public HammingSimilarity() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.CharSequence,
	 * java.lang.CharSequence)
	 */
	@Override
	public float getSimilarity(CharSequence text1, CharSequence text2) {
		return 1 - (float) computeDistance(text1, text2) / Math.max(text1.length(), text2.length());
	}

	private int computeDistance(CharSequence str1, CharSequence str2) {
		final int len = Math.min(str1.length(), str2.length());

		int distance = 0;
		for (int i = 0; i < len; i++)
			if (str1.charAt(i) != str2.charAt(i))
				distance++;

		return distance;
	}
}
