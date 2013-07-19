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
package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
public class RuleBasedFusion extends CompositeOperator<RuleBasedFusion> {
	private ConflictResolution conflictResolution = new MergeRule();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator.SopremoModule ,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		JsonStream pipeline = module.getInput(0);

		// wrap in array
		// pipeline = new
		// Projection().withInputs(pipeline).withResultProjection(new
		// UnionObjects());

		pipeline = new Projection().withInputs(pipeline).withResultProjection(this.conflictResolution);

		// unwrap in array
		// pipeline = new
		// Projection().withInputs(pipeline).withResultProjection(new arrayUn);

		pipeline = new Selection().withInputs(pipeline).withCondition(UnaryExpression.not(new ValueTreeContains(FilterRecord.Instance)));

		module.getOutput(0).setInput(0, pipeline);
	}

	public static class FusionProjection extends Projection {
		@Override
		public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
			return super.asPactModule(new FusionContext(context), layout);
		}
	}
}
