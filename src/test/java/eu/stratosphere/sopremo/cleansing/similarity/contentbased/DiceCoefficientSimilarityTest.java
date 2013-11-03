package eu.stratosphere.sopremo.cleansing.similarity.contentbased;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityBaseTest;
import eu.stratosphere.sopremo.cleansing.similarity.set.DiceCoefficientSimilarity;

/**
 * Tests the {@link DiceCoefficientSimilarity} class.
 * 
 * @author Arvid Heise
 */
public class DiceCoefficientSimilarityTest extends SimilarityBaseTest {
	public DiceCoefficientSimilarityTest(Object node1, Object node2, Object expected) {
		super(node1, node2, expected);
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.dude.junit.Similarity.SimilarityBaseTest#getSimilarity()
	 */
	@Override
	public Similarity<?> getSimilarity() {
		return new DiceCoefficientSimilarity();
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "thomas", "thomas", withTokenization(1) },
			{ "hans-peter", "hans-peter", withTokenization(1) },
			{ "hans-peter", "Hans-peter", withTokenization(0) },
			{ "hans peter", "Hans peter", withTokenization(0.5) },
			{ "thomas", "", withTokenization(0) },
			{ "thomas", null, withTokenization(0) },
			{ "", "", withTokenization(1) },
			{ "", null, withTokenization(1) },
		});
	}
}