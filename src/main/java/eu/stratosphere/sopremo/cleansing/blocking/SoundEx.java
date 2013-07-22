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

package eu.stratosphere.sopremo.cleansing.blocking;

import java.io.IOException;

/**
 * <code>SoundEx</code> implements a phonetic algorithm for indexing names by sound.
 * 
 * @author Matthias Pohl
 * @author Arvid Heise
 */
public class SoundEx {

	private static final char[] MAP = {
		/*-  
		 A    B    C    D    E    F    G    H    I    J    K    L    M*/
		'0', '1', '2', '3', '0', '1', '2', '0', '0', '2', '2', '4', '5',
		/*-  
		 N    O    P    W    R    S    T    U    V    W    X    Y    Z*/
		'5', '0', '1', '2', '6', '2', '3', '0', '1', '0', '2', '0', '2' };

	/**
	 * Generates the <code>SoundEx</code> value of the passed String.
	 * 
	 * @param s
	 *        The String whose <code>SoundEx</code> value shall be generated.
	 * @return The <code>SoundEx</code> value of the passed String.
	 */
	public static void generateSoundExInto(CharSequence input, Appendable soundex) throws IOException {
		/*
		 * This code fragment was copied from http://www.java-forums.org/java-lang
		 * /7438-soundex-algorithm-implementation-java.html
		 */

		// Algorithm works on uppercase (mainframe era).
		char c, prev = '?';

		// Main loop: find up to 4 chars that map.
		int soundexLength = 0;
		for (int inputPos = 0; inputPos < input.length() && soundexLength < 4; inputPos++) {
			c = Character.toUpperCase(input.charAt(inputPos));

			// Check to see if the given character is alphabetic.
			// Text is already converted to uppercase. Algorithm
			// only handles ASCII letters, do NOT use Character.isLetter()!
			// Also, skip double letters.
			if (c >= 'A' && c <= 'Z' && c != prev) {
				prev = c;

				// First char is installed unchanged, for sorting.
				if (inputPos == 0) {
					soundex.append(c);
					soundexLength++;
				} else {
					char m = MAP[c - 'A'];
					if (m != '0') {
						soundex.append(m);
						soundexLength++;
					}
				}
			}
		}
	}

}
