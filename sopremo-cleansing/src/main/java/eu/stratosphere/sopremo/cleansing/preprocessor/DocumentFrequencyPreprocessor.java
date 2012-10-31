package eu.stratosphere.sopremo.cleansing.preprocessor;

import java.util.HashMap;

/**
 * 
 * The <code>DocumentFrequencyPreprocessor</code> collects frequencies of terms within an attribute value. Each value from the considered attribute is
 * regarded as a document.
 * 
 * @author ziawasch.abedjan
 * 
 */
public class DocumentFrequencyPreprocessor implements Preprocessor {
	private String attributeName;
	private int documentCount = 0;
	private HashMap<String, Integer> documentFrequencies = new HashMap<String, Integer>();

	/**
	 * Initializes a <code>DocumentFrequencyPreprocessor</code> object for the passed attribute.
	 * 
	 * @param attrName
	 *            The attribute on which the document frequencies are calculated.
	 */
	public DocumentFrequencyPreprocessor(String attrName) {
		this.attributeName = attrName;
	}

	/**
	 * Retrieves the value frequencies within the considered attribute and ads them to the total document frequency of the terms
	 */
	@Override
	public void analyzeIJsonNode(IJsonNode data) {
		this.documentCount++;
		String value = getStringifiedAttributeValue(data);
		if (value != null) {
			String[] terms = value.split(" ");
			for (String term : terms) {
				if (this.documentFrequencies.containsKey(term)) {
					int freq = this.documentFrequencies.get(term);
					this.documentFrequencies.put(term, ++freq);
				} else {
					this.documentFrequencies.put(term, 1);
				}
			}
		}

	}
	/**
	 * Does nothing.
	 */
	@Override
	public void clearData() {
		// nothing to do

	}
	/**
	 * Does nothing
	 */
	@Override
	public void finish() {
		// nothing to do

	}

	/**
	 * Retrieves the inverse document frequency of the passed term. The document frequency is the number of total occurences of the given term within
	 * values of the considered attribute.
	 * 
	 * @param term
	 *            The considered term
	 * @return Inverse document frequency log(N/df)
	 */
	public double getInverseDocumentFrequency(String term) {

		return Math.log10((double) this.documentCount / (double) this.documentFrequencies.get(term));
	}

	/**
	 * Returns a concatenation of all values stored in the object's attribute.
	 * 
	 * @param obj
	 *            The object whose values specified by the attribute name will be returned as a String.
	 * @return The concatenation of all values stored within the object's attribute.
	 */
	private String getStringifiedAttributeValue(IJsonNode obj) {
		if (obj == null) {
			return null;
		}

		JsonArray array = obj.getAttributeValues(this.attributeName);

		if (array == null) {
			return null;
		}

		return array.generateString();
	}

}
