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
import java.io.ObjectInputStream;
import java.util.BitSet;

import eu.stratosphere.sopremo.AbstractSopremoType;

/**
 * <code>JaroSimilarity</code> compares two {@link IJsonNode}s based on the Jaro Distance attribute.
 * 
 * @author Arvid Heise
 */
public class JaroSimilarity extends TextSimilarity {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6164950344618325118L;

	protected transient BitSet leftMatched = new BitSet(), rightMatched = new BitSet();

	protected transient int commonChars, transpositions;

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.leftMatched = new BitSet();
		this.rightMatched = new BitSet();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.CharSequence,
	 * java.lang.CharSequence, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(CharSequence text1, CharSequence text2) {
		this.leftMatched.clear();
		this.rightMatched.clear();

		findCommonChars(text1, text2);

		/* If no characters in common - return */
		if (this.commonChars == 0)
			return 0;

		countTranspositions(text1, text2);

		double dCommonChars = this.commonChars;
		return (dCommonChars / text1.length() + dCommonChars / text2.length() + //
		(this.commonChars - this.transpositions) / dCommonChars) / 3;
	}

	private void countTranspositions(CharSequence text1, CharSequence text2) {
		/* Count the number of transpositions */
		int halfTranspositions = 0, pos1 = -1, pos2 = -1;
		while ((pos1 = this.leftMatched.nextSetBit(pos1 + 1)) != -1) {
			pos2 = this.rightMatched.nextSetBit(pos2 + 1);
			if (text1.charAt(pos1) != text2.charAt(pos2))
				halfTranspositions++;
		}
		this.transpositions = halfTranspositions / 2;
	}

	private void findCommonChars(CharSequence text1, CharSequence text2) {
		/* Looking only within the search range, count and flag the matched pairs. */
		this.commonChars = 0;
		final int len1 = text1.length(), len2 = text2.length();
		final int searchRange = Math.max(Math.max(len1, len2) / 2 - 1, 0);
		for (int pos1 = 0; pos1 < len1; pos1++) {
			final int maxPos = Math.min(searchRange + pos1, len2 - 1);

			final char unmatchedChar1 = text1.charAt(pos1);
			int pos2 = Math.max(pos1 - searchRange, 0) - 1;
			for (; (pos2 = this.rightMatched.nextClearBit(pos2 + 1)) <= maxPos;) {
				if (text2.charAt(pos2) == unmatchedChar1) {
					this.commonChars++;
					this.leftMatched.set(pos1);
					this.rightMatched.set(pos2);
					break;
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new JaroWinklerSimilarity();
	}
}