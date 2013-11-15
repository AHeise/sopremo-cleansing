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
 * <code>LevenshteinSimilarity</code> compares two {@link IJsonNode}s based on the Levenshtein Distance.
 * 
 * @author Arvid Heise
 */
public class LevenshteinSimilarity extends TextSimilarity {
	/**
	 * Initializes a <code>LevenshteineSimilarity</code>.
	 */
	public LevenshteinSimilarity() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.CharSequence,
	 * java.lang.CharSequence)
	 */
	@Override
	public float getSimilarity(CharSequence text1, CharSequence text2) {
		return 1 - (float) computeLevenshteinDistance(text1, text2) / Math.max(text1.length(), text2.length());
	}

	private static int min(int a, int b, int c) {
		return Math.min(Math.min(a, b), c);
	}

	private int[][] distanceMatrix = new int[0][0];

	private int computeLevenshteinDistance(CharSequence str1, CharSequence str2) {
		int len1 = str1.length();
		int len2 = str2.length();
		// resize the matrix to fit both strings
		if (this.distanceMatrix.length <= len1 || this.distanceMatrix[0].length <= len2)
			this.distanceMatrix = new int[Math.max(len1 + 1, this.distanceMatrix.length)]
				[this.distanceMatrix.length > 0 ? Math.max(len2 + 1, this.distanceMatrix[0].length) : len2 + 1];

		for (int i = 0; i <= len1; i++)
			this.distanceMatrix[i][0] = i;
		for (int j = 1; j <= len2; j++)
			this.distanceMatrix[0][j] = j;

		for (int i = 1; i <= len1; i++)
			for (int j = 1; j <= len2; j++)
				this.distanceMatrix[i][j] = min(
					this.distanceMatrix[i - 1][j] + 1,
					this.distanceMatrix[i][j - 1] + 1,
					this.distanceMatrix[i - 1][j - 1]
						+ ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0
							: 1));

		return this.distanceMatrix[len1][len2];
	}
}
