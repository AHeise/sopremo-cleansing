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

package eu.stratosphere.sopremo.cleansing.similarity.contentbased;

import java.util.Arrays;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityBaseTest;
import eu.stratosphere.sopremo.cleansing.similarity.text.IdentitySimilarity;

/**
 * Tests the {@link IdentitySimilarity} class.
 * 
 * @author Arvid Heise
 */
public class IdentitySimilarityTest extends SimilarityBaseTest {
	public IdentitySimilarityTest(Object node1, Object node2, double expected) {
		super(node1, node2, expected);
	}

	/* (non-Javadoc)
	 * @see de.hpi.fgis.dude.junit.Similarity.SimilarityBaseTest#getSimilarity()
	 */
	@Override
	public Similarity<?> getSimilarity() {
		return new IdentitySimilarity();
	}
	
	@Parameters
	public static  List<Object[]>  getParameters() {
		return Arrays.asList(new Object[][] {
			{ "thomas", "thomas", 1 },
			{ "hans-peter", "hans-peter", 1 },
			{ "hans-peter", "Hans-peter", 0 },
			{ "thomas", "", 0 },
			{ "thomas", null, withCoercion(0) },
			{ "", "", 1 },
			{ "", null, withCoercion(0) },
		});
	}
}
