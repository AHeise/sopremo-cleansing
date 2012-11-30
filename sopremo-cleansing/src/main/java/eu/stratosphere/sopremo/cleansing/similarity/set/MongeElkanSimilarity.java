package eu.stratosphere.sopremo.cleansing.similarity.set;

import java.io.IOException;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.cleansing.similarity.Asymmetric;
import eu.stratosphere.sopremo.cleansing.similarity.CoercingSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.LevenshteinSimilarity;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@Asymmetric
public class MongeElkanSimilarity extends SetSimilarity {
	/**
	 * 
	 */
	private static final Similarity<IJsonNode> DEFAULT_MEASURE = new CoercingSimilarity(new LevenshteinSimilarity());

	/**
	 * 
	 */
	private static final long serialVersionUID = 1688822374984917780L;

	private Similarity<IJsonNode> baseMeasure = DEFAULT_MEASURE;

	public MongeElkanSimilarity(Similarity<? extends IJsonNode> baseMeasure) {
		this.baseMeasure = (Similarity<IJsonNode>) baseMeasure;
	}
	
	/**
	 * Initializes MongeElkanSimilarity.
	 *
	 */
	public MongeElkanSimilarity() {
	}

	public Similarity<IJsonNode> getBaseMeasure() {
		return this.baseMeasure;
	}

	public void setBaseMeasure(Similarity<IJsonNode> baseMeasure) {
		if (baseMeasure == null)
			throw new NullPointerException("baseMeasure must not be null");

		this.baseMeasure = baseMeasure;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.set.SetSimilarity#getSetSimilarity(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double getSetSimilarity(IArrayNode leftValues, IArrayNode rightValues) {
		double sum = 0;
		for (IJsonNode leftValue : leftValues) {
			double max = 0;
			for (IJsonNode rightValue : rightValues)
				max = Math.max(max, this.baseMeasure.getSimilarity(leftValue, rightValue));
			sum += max;
		}

		return sum / leftValues.size();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.baseMeasure.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		MongeElkanSimilarity other = (MongeElkanSimilarity) obj;
		return this.baseMeasure.equals(other.baseMeasure);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new MongeElkanSimilarity(baseMeasure.clone());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("monge elkan of ");
		this.baseMeasure.appendAsString(appendable);
	}
}
