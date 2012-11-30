/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.similarity;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class SimilarityFactory {
	public final static SimilarityFactory INSTANCE = new SimilarityFactory();
	
	@SuppressWarnings("unchecked")
	public Similarity<?> create(final Similarity<?> similarity, final EvaluationExpression leftPath,
			final EvaluationExpression rightPath, final boolean coercion) {
		Similarity<?> baseSimilarity = similarity;
		PathSegmentExpression left = (PathSegmentExpression) leftPath;
		PathSegmentExpression right = (PathSegmentExpression) rightPath;
		boolean shouldCoerce = coercion;

		while (true) {
			if (baseSimilarity instanceof PathSimilarity<?>) {
				final PathSimilarity<?> pathSimilarity = (PathSimilarity<?>) baseSimilarity;
				baseSimilarity = pathSimilarity.getActualSimilarity();
				left = pathSimilarity.getLeftExpression().clone().withTail(left);
				right = pathSimilarity.getRightExpression().clone().withTail(left);
			} else if (baseSimilarity instanceof CoercingSimilarity) {
				baseSimilarity = ((CoercingSimilarity) baseSimilarity).getActualSimilarity();
				shouldCoerce = true;
			} else
				break;
		}

		if (shouldCoerce)
			baseSimilarity = new CoercingSimilarity(baseSimilarity);

		if (left != EvaluationExpression.VALUE || right != EvaluationExpression.VALUE)
			baseSimilarity = new PathSimilarity<IJsonNode>(left, (Similarity<IJsonNode>) baseSimilarity, right);

		return baseSimilarity;
	}
}
