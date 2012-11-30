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

import uk.ac.shef.wit.simmetrics.math.MathFuncs;
import uk.ac.shef.wit.simmetrics.similaritymetrics.costfunctions.AbstractSubstitutionCost;
import uk.ac.shef.wit.simmetrics.similaritymetrics.costfunctions.SubCost1_Minus2;
import eu.stratosphere.sopremo.AbstractSopremoType;

/**
 * <code>SmithWatermanSimilarity</code> compares two {@link IJsonNode}s based on the Smith Waterman Distance attribute.
 * 
 * @author Arvid Heise
 */
public class SmithWatermanSimilarity extends TextSimilarity {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2166171739070205462L;

	/**
	 * the private cost function used in the levenstein distance.
	 */
	private AbstractSubstitutionCost dCostFunc;

	/**
	 * the cost of a gap.
	 */
	private float gapCost;

	/**
	 * constructor - default (empty).
	 */
	public SmithWatermanSimilarity() {
		// set the gapCost to a default value
		this.gapCost = 0.5f;
		// set the default cost func
		this.dCostFunc = new SubCost1_Minus2();
	}

	/**
	 * gets the gap cost for the distance function.
	 * 
	 * @return the gap cost for the distance function
	 */
	public float getGapCost() {
		return this.gapCost;
	}

	/**
	 * sets the gap cost for the distance function.
	 * 
	 * @param gapCost
	 *        the cost of a gap
	 */
	public void setGapCost(final float gapCost) {
		this.gapCost = gapCost;
	}

	/**
	 * get the d(i,j) cost function.
	 * 
	 * @return AbstractSubstitutionCost cost function used
	 */
	public AbstractSubstitutionCost getdCostFunc() {
		return this.dCostFunc;
	}

	/**
	 * sets the d(i,j) cost function used.
	 * 
	 * @param dCostFunc
	 *        - the cost function to use
	 */
	public void setdCostFunc(final AbstractSubstitutionCost dCostFunc) {
		this.dCostFunc = dCostFunc;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.String,
	 * java.lang.String, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(CharSequence text1, CharSequence text2) {
		final double smithWaterman = this.getUnNormalisedSimilarity(text1.toString(), text2.toString());

		// normalise into zero to one region from min max possible
		final int maxLength = Math.max(text1.length(), text2.length());
		final double maxValue = maxLength * Math.max(this.dCostFunc.getMaxCost(), this.gapCost);
		final double minValue = maxLength * Math.min(this.dCostFunc.getMinCost(), this.gapCost);

		// return actual / possible distance to get 0-1 range
		return (smithWaterman - minValue) / (maxValue - minValue);
	}

	/**
	 * implements the Smith-Waterman distance function
	 * //see http://www.gen.tcd.ie/molevol/nwswat.html for details .
	 * 
	 * @param s
	 * @param t
	 * @return the Smith-Waterman distance for the given strings
	 */
	private float getUnNormalisedSimilarity(final String s, final String t) {
		final float[][] d; // matrix
		final int n; // length of s
		final int m; // length of t
		int i; // iterates through s
		int j; // iterates through t
		float cost; // cost

		// check for zero length input
		n = s.length();
		m = t.length();

		if (m == 0 || n == 0)
			return 0;

		// create matrix (n)x(m)
		d = new float[n][m];

		// process first row and column first as no need to consider previous rows/columns
		float maxSoFar = 0.0f;
		for (i = 0; i < n; i++) {
			// get the substution cost
			cost = this.dCostFunc.getCost(s, i, t, 0);

			if (i == 0)
				d[0][0] = MathFuncs.max3(0,
					-this.gapCost,
					cost);
			else
				d[i][0] = MathFuncs.max3(0,
					d[i - 1][0] - this.gapCost,
					cost);
			// update max possible if available
			if (d[i][0] > maxSoFar)
				maxSoFar = d[i][0];
		}
		for (j = 0; j < m; j++) {
			// get the substution cost
			cost = this.dCostFunc.getCost(s, 0, t, j);

			if (j == 0)
				d[0][0] = MathFuncs.max3(0,
					-this.gapCost,
					cost);
			else
				d[0][j] = MathFuncs.max3(0,
					d[0][j - 1] - this.gapCost,
					cost);
			// update max possible if available
			if (d[0][j] > maxSoFar)
				maxSoFar = d[0][j];
		}

		// cycle through rest of table filling values from the lowest cost value of the three part cost function
		for (i = 1; i < n; i++)
			for (j = 1; j < m; j++) {
				// get the substution cost
				cost = this.dCostFunc.getCost(s, i, t, j);

				// find lowest cost at point from three possible
				d[i][j] = MathFuncs.max4(0,
					d[i - 1][j] - this.gapCost,
					d[i][j - 1] - this.gapCost,
					d[i - 1][j - 1] + cost);
				// update max possible if available
				if (d[i][j] > maxSoFar)
					maxSoFar = d[i][j];
			}

		// return max value within matrix as holds the maximum edit score
		return maxSoFar;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new SmithWatermanSimilarity();
	}
}