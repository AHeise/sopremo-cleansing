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

package eu.stratosphere.sopremo.cleansing.preprocessor;


/**
 * <code>DataCountPreprocessor</code> is a sample class, that shows how the {@link Preprocessor} interface can be
 * used. Since {@link #analyzeIJsonNode(IJsonNode)} is called once for each extracted {@link IJsonNode} we can gather
 * statistics within the extraction phase. This sample class implements an object count. It simply counts the
 * <code>analyzeIJsonNode(IJsonNode)</code> calls. The object count can be used later on by calling
 * {@link #getObjectCount()} within the algorithm.
 * 
 * @author Matthias Pohl
 */
public class DataCountPreprocessor implements Preprocessor {

	private int objectCount;

	/**
	 * Initializes a <code>DataCountPreprocessor</code>.
	 */
	public DataCountPreprocessor() {
		this.clearData();
	}

	/**
	 * {@inheritDoc AbstractDataPreprocessor#analyzeIJsonNode(IJsonNode)}
	 */
	@Override
	public void analyzeIJsonNode(IJsonNode data) {
		++this.objectCount;
	}

	/**
	 * Returns the number of objects that were extracted during the data extraction phase.
	 * 
	 * @return The number of extracted {@link IJsonNode}s.
	 */
	public int getObjectCount() {
		return this.objectCount;
	}

	/**
	 * {@inheritDoc Preprocessor#clearData()}
	 */
	@Override
	public void clearData() {
		this.objectCount = 0;
	}

	/**
	 * {@inheritDoc Preprocessor#finish()}
	 */
	@Override
	public void finish() {
		// nothing to do
	}

}
