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

import java.util.HashMap;
import java.util.HashSet;

import eu.stratosphere.sopremo.cleansing.preprocessor.DocumentFrequencyPreprocessor;
import eu.stratosphere.sopremo.cleansing.similarity.Requires;
import eu.stratosphere.sopremo.cleansing.similarity.Statistics;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <code>TFIDFSimilarity</code> compares two {@link IJsonNode}s based on the classic tf-idf metric if idf preprocessing
 * is activated. Otherwise it
 * compares the cosine similarity based on tf vectors.
 * 
 * @author ziawasch.abedjan
 */
@Requires(Statistics.class)
public class TFIDFSimilarity extends TextSimilarity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1724957156375152345L;

	private DocumentFrequencyPreprocessor idf = new DocumentFrequencyPreprocessor(null);

	@Override
	public double compareObjects(IJsonNode obj1, IJsonNode obj2) {
		final String str1 = this.getStringifiedAttributeValue(obj1);
		final String str2 = this.getStringifiedAttributeValue(obj2);

		if (str1 == null || str2 == null)
			return 0.0;

		final String[] tokens1Strings = str1.split(" ");
		final String[] tokens2Strings = str2.split(" ");

		final HashMap<String, Integer> termFrequencies1 = new HashMap<String, Integer>();
		final HashMap<String, Integer> termFrequencies2 = new HashMap<String, Integer>();
		final HashSet<String> totalterms = new HashSet<String>();
		// retrieve term frequencies for terms in obj1
		for (final String term : tokens1Strings)
			if (termFrequencies1.containsKey(term)) {
				int frequency = termFrequencies1.get(term);
				frequency++;
				termFrequencies1.put(term, frequency);
			} else {
				termFrequencies1.put(term, 1);
				totalterms.add(term);
			}
		// retrieve term frequencies for terms in obj2
		for (final String term : tokens2Strings)
			if (termFrequencies2.containsKey(term)) {
				int frequency = termFrequencies2.get(term);
				frequency++;
				termFrequencies2.put(term, frequency);
			} else {
				termFrequencies2.put(term, 1);
				totalterms.add(term);
			}
		// begin computation
		double dotproduct = 0.0;
		double vectorlength1 = 0.0;
		double vectorlength2 = 0.0;
		// compute withoud idf if there are no document frequencies available
		if (this.idf == null) {
			// compute the numerator of the cosine similarity
			for (final String term : totalterms)
				dotproduct =
					dotproduct + this.getWeight(termFrequencies1, term) * this.getWeight(termFrequencies2, term);
			// compute the denominator factors
			for (final String term : termFrequencies1.keySet()) {
				final double freq = termFrequencies1.get(term);
				vectorlength1 = vectorlength1 + freq * freq;
			}

			for (final String term : termFrequencies2.keySet()) {
				final double freq = termFrequencies2.get(term);
				vectorlength2 = vectorlength2 + freq * freq;
			}
		} else {
			// compute the numerator of the cosine similarity
			for (final String term : totalterms)
				dotproduct =
					dotproduct + this.getWeight(termFrequencies1, term) * this.getWeight(termFrequencies2, term)
						* this.idf.getInverseDocumentFrequency(term) * this.idf.getInverseDocumentFrequency(term);

			// Compute the denominator factors.
			for (final String term : termFrequencies1.keySet()) {
				final double freq = termFrequencies1.get(term);
				vectorlength1 =
					vectorlength1 + freq * freq * this.idf.getInverseDocumentFrequency(term)
						* this.idf.getInverseDocumentFrequency(term);
			}

			for (final String term : termFrequencies2.keySet()) {
				final double freq = termFrequencies2.get(term);
				vectorlength2 =
					vectorlength2 + freq * freq * this.idf.getInverseDocumentFrequency(term)
						* this.idf.getInverseDocumentFrequency(term);
			}
		}
		final double denominator = Math.sqrt(vectorlength1 * vectorlength2);
		return dotproduct / denominator;
	}

	private double getWeight(HashMap<String, Integer> map, String term) {
		if (map.containsKey(term))
			return map.get(term);
		return 0;
	}

	@Override
	public String toString() {
		return "TFIDFSimilarity []";
	}

}
