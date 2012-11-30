package eu.stratosphere.sopremo.cleansing.similarity.contentbased;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityBaseTest;
import eu.stratosphere.sopremo.cleansing.similarity.text.TFIDFSimilarity;

/**
 * Tests the {@link TFIDFSimilarityTest} class.
 * 
 * @author Arvid Heise
 */
@Ignore
public class TFIDFSimilarityTest extends SimilarityBaseTest {
	public TFIDFSimilarityTest(Object node1, Object node2, double expected) {
		super(node1, node2, expected);
	}

	/*
	 * (non-Javadoc)
	 * @see de.hpi.fgis.dude.junit.Similarity.SimilarityBaseTest#getSimilarity()
	 */
	@Override
	public Similarity<?> getSimilarity() {
		return new TFIDFSimilarity();
	}

	@Parameters
	public static List<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "thomas", "thomas", 0 },
			{ "hans-peter", "hans-peter", 0 },
			{ "hans-peter", "Hans-peter", 0 },
			{ "thomas", "", 0 },
			{ "thomas", null, withCoercion(0) },
			{ "", "", 1 },
			{ "", null, withCoercion(0) },
		});
	}
//
//	/**
//	 * A <code>IJsonNode</code> containing one filled attribute consisting of three tokens.
//	 */
//	protected IJsonNode thomasJerimyRonaldObject;
//
//	/**
//	 * A <code>IJsonNode</code> containing one filled attribute consisting of three tokens.
//	 */
//	protected IJsonNode thomasJerimyPascalObject;
//
//	/**
//	 * The preset of each test method.
//	 * 
//	 * @throws Exception
//	 *         If an error occurs while initializing the test methods.
//	 */
//	@Override
//	@Before
//	public void setUp() throws Exception {
//		super.setUp();
//		JsonRecord data = new JsonRecord();
//		data.put(this.attributeName, new JsonArray(new JsonString("Thomas Jerimy Pascal")));
//
//		this.thomasJerimyPascalObject = new IJsonNode(data, "SimilarityTest", "Thomas-Jerimy-Pascal");
//
//		data = new JsonRecord();
//		data.put(this.attributeName, new JsonArray(new JsonString("Thomas Jerimy Ronald")));
//
//		this.thomasJerimyRonaldObject = new IJsonNode(data, "SimilarityTest", "Thomas-Jerimy-Ronald");
//	}
//
//	/**
//	 * Tests {@link TFIDFSimilarityTest#compareObjects(IJsonNode, IJsonNode)}.
//	 */
//	@Test
//	public void testCompareObjectsIJsonNodeIJsonNode() {
//		TFIDFSimilarity Similarity = new TFIDFSimilarity(
//			this.attributeName);
//
//		assertEquals(1.0, Similarity.compareObjects(this.thomasObject,
//			this.thomasObject), 0.0);
//		assertEquals(1.0, Similarity.compareObjects(this.hansPeterObject,
//			this.hansPeterObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.thomasObject), 0.0);
//
//		assertEquals(1.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.noAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.emptyAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.thomasObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.thomasObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(1.0 / Math.sqrt(3.0), Similarity.compareObjects(this.thomasObject,
//			this.thomasJerimyPascalObject), 0.0);
//		assertEquals(1.0 / Math.sqrt(3.0), Similarity.compareObjects(this.thomasObject,
//			this.thomasJerimyRonaldObject), 0.0);
//		assertEquals(2.0 / 3.0, Similarity.compareObjects(this.thomasJerimyPascalObject,
//			this.thomasJerimyRonaldObject), 0.0);
//		assertEquals(1.0, Similarity.compareObjects(this.thomasJerimyRonaldObject,
//			this.thomasJerimyRonaldObject), 0.0);
//
//	}
//
//	/**
//	 * Tests having idf preprocessing information {@link TFIDFSimilarityTest#compareObjects(IJsonNode, IJsonNode)}.
//	 */
//	@Test
//	public void testCompareObjectsIJsonNodeIJsonNodeWithIDF() {
//		DocumentFrequencyPreprocessor dfp = new DocumentFrequencyPreprocessor(this.attributeName);
//		dfp.analyzeIJsonNode(this.hansPeterObject);
//		dfp.analyzeIJsonNode(this.emptyAttributeObject);
//		dfp.analyzeIJsonNode(this.noAttributeObject);
//		dfp.analyzeIJsonNode(this.thomasObject);
//		dfp.analyzeIJsonNode(this.thomasJerimyPascalObject);
//		dfp.analyzeIJsonNode(this.thomasJerimyRonaldObject);
//
//		assertEquals(Math.log10(6.0 / 3.0), dfp.getInverseDocumentFrequency("Thomas"), 0.0);
//		assertEquals(Math.log10(6.0 / 1.0), dfp.getInverseDocumentFrequency("HansPeter"), 0.0);
//		assertEquals(Math.log10(6.0 / 2.0), dfp.getInverseDocumentFrequency("Jerimy"), 0.0);
//		assertEquals(Math.log10(6.0 / 1.0), dfp.getInverseDocumentFrequency("Pascal"), 0.0);
//		TFIDFSimilarity Similarity = new TFIDFSimilarity(
//			this.attributeName, dfp);
//
//		assertEquals(1.0, Similarity.compareObjects(this.thomasObject,
//			this.thomasObject), 0.0);
//		assertEquals(1.0, Similarity.compareObjects(this.hansPeterObject,
//			this.hansPeterObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.thomasObject), 0.0);
//
//		assertEquals(1.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.noAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.emptyAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.thomasObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.thomasObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.thomasObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(0.0, Similarity.compareObjects(this.emptyAttributeObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.emptyAttributeObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.noAttributeObject,
//			this.hansPeterObject), 0.0);
//		assertEquals(0.0, Similarity.compareObjects(this.hansPeterObject,
//			this.noAttributeObject), 0.0);
//
//		assertEquals(
//			Math.log10(6.0 / 3.0)
//				* Math.log10(6.0 / 3.0)
//				/ Math.sqrt(Math.log10(6.0 / 3.0)
//					* Math.log10(6.0 / 3.0)
//					* (Math.log10(6.0 / 3.0) * Math.log10(6.0 / 3.0) + Math.log10(6.0 / 2.0) * Math.log10(6.0 / 2.0) + Math.log10(6.0 / 1.0)
//						* Math.log10(6.0 / 1.0))), Similarity.compareObjects(this.thomasObject,
//				this.thomasJerimyPascalObject), 0.0);
//		assertEquals(
//			Math.log10(6.0 / 3.0)
//				* Math.log10(6.0 / 3.0)
//				/ Math.sqrt(Math.log10(6.0 / 3.0)
//					* Math.log10(6.0 / 3.0)
//					* (Math.log10(6.0 / 3.0) * Math.log10(6.0 / 3.0) + Math.log10(6.0 / 2.0) * Math.log10(6.0 / 2.0) + Math.log10(6.0 / 1.0)
//						* Math.log10(6.0 / 1.0))), Similarity.compareObjects(this.thomasObject,
//				this.thomasJerimyRonaldObject), 0.0);
//		assertEquals(
//			(Math.log10(6.0 / 3.0) * Math.log10(6.0 / 3.0) + Math.log10(6.0 / 2.0) * Math.log10(6.0 / 2.0))
//				/ (Math.log10(6.0 / 3.0) * Math.log10(6.0 / 3.0) + Math.log10(6.0 / 2.0) * Math.log10(6.0 / 2.0) + Math.log10(6.0 / 1.0)
//					* Math.log10(6.0 / 1.0)), Similarity.compareObjects(this.thomasJerimyPascalObject,
//				this.thomasJerimyRonaldObject), 0.0);
//		assertEquals(1.0, Similarity.compareObjects(this.thomasJerimyRonaldObject,
//			this.thomasJerimyRonaldObject), 0.0);
//
//	}

}