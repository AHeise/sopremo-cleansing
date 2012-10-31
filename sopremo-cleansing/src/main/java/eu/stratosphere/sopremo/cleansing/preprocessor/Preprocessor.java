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
 * <code>Preprocessor</code> is an <code>interface</code> that can be used for gathering statistics of the data
 * within the extraction phase.
 * 
 * @author Matthias Pohl
 */
public interface Preprocessor {

	/**
	 * Passes the currently extracted {@link IJsonNode} to the <code>Preprocessor</code> for further analysis. This
	 * method is called by every {@link DataExtractor} per extracted data record.
	 * 
	 * @param data
	 *            The <code>IJsonNode</code> that shall be analyzed.
	 */
	public void analyzeIJsonNode(final IJsonNode data);

	/**
	 * This method is called after finishing the data extraction process. It can be used in order to created some
	 * further statistics.
	 */
	public void finish();

	/**
	 * Clears statistics that were already gathered.
	 */
	public void clearData();

}
