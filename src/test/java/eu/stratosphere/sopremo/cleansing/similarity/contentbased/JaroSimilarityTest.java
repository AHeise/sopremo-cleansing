package eu.stratosphere.sopremo.cleansing.similarity.contentbased;

import java.util.Arrays;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityBaseTest;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroSimilarity;

/**
 * Tests the {@link JaroDistanceSimilarity} class.
 * 
 * @author Arvid Heise
 */
public class JaroSimilarityTest extends SimilarityBaseTest {
	public JaroSimilarityTest(Object node1, Object node2, Object expected) {
		super(node1, node2, expected);
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.dude.junit.Similarity.SimilarityBaseTest#getSimilarity()
	 */
	@Override
	public Similarity<?> getSimilarity() {
		return new JaroSimilarity();
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "thomas", "thomas", 1 },
			{ "hans-peter", "hans-peter", 1 },
			{ "hans-peter", "Hans-peter", (0.9 + 0.9 + 1) / 3 },
			{ "martha", "marhta", (1 + 1 + 5d / 6) / 3 },
			{ "jones", "johnson", (4d / 5 + 4d / 7 + 1) / 3 },
			{ "abcvwxyz", "cabvwxyz", (1 + 1 + 7d / 8) / 3 },
			{ "thomas", "", 0 },
			{ "thomas", null, withCoercion(0) },
			{ "", "", 1 },
			{ "", null, withCoercion(1) },
		});
	}
}