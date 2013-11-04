package eu.stratosphere.sopremo.cleansing.similarity.contentbased;

import java.util.Arrays;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityBaseTest;
import eu.stratosphere.sopremo.cleansing.similarity.set.MongeElkanSimilarity;

/**
 * Tests the {@link MongeElkanSimilarity} class.
 * 
 * @author Arvid Heise
 */
public class MongeElkanSimilarityTest extends SimilarityBaseTest {
	public MongeElkanSimilarityTest(Object node1, Object node2, Object expected) {
		super(node1, node2, expected);
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.dude.junit.Similarity.SimilarityBaseTest#getSimilarity()
	 */
	@Override
	public Similarity<?> getSimilarity() {
		return new MongeElkanSimilarity();
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "thomas", "thomas", withCoercion(1) },
			{ "hans-peter", "hans-peter", withCoercion(1) },
			{ "hans-peter", "Hans-peter", withCoercion(0.9) },
			{ "thomas", "", withCoercion(0) },
			{ "thomas", null, withCoercion(0) },
			{ new Object[] { "John", "Kennedy" }, new Object[] { "Jon", "F.", "Kennidy" }, (3d / 4 + 6d / 7) / 2 },
			{ "", "", withCoercion(1) },
			{ "", null, withCoercion(1) },
		});
	}
}