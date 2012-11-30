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

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Basic adaptor for all simmetrics metrics.
 * 
 * @author Arvid Heise
 * @param <M>
 *        the simmetrics metric
 */
public abstract class SimmetricsSimilarity<M extends AbstractStringMetric> extends TextSimilarity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3799657655086627167L;

	private M metric;

	/**
	 * Initializes a <code>SimmetricsSimilarity</code> with the given metric and for the passed attribute.
	 * 
	 * @param metric
	 *        The simmetric metric
	 */
	protected SimmetricsSimilarity(M metric) {
		this.metric = metric;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.String,
	 * java.lang.String, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(CharSequence text1, CharSequence text2) {
		return this.metric.getSimilarity(text1.toString(), text2.toString());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return ReflectUtil.newInstance(getClass());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final SimmetricsSimilarity<?> other = (SimmetricsSimilarity<?>) obj;
		return this.metric.getClass().equals(other.metric.getClass());
	}

	/**
	 * Returns the metric
	 * 
	 * @return the simmetric metric
	 */
	protected M getMetric() {
		return this.metric;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.metric.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		SopremoUtil.append(appendable, " [metric=", this.metric, "]");
	}
}
