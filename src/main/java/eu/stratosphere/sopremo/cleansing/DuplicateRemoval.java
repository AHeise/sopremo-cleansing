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
package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.base.ArraySplit;
import eu.stratosphere.sopremo.base.Difference;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.operator.*;

/**
 * Detects duplicates, fuses them, and combines the non-duplicates the the fused records.
 * 
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
@Name(verb = "remove duplicates")
public class DuplicateRemoval extends CompositeOperator<DuplicateRemoval> {
	private final DuplicateDetection duplicateDetection = new DuplicateDetection();

	private final Fusion fusion = new Fusion();

	/**
	 * Initializes DuplicateRemoval.
	 */
	public DuplicateRemoval() {
		addPropertiesFrom(this.duplicateDetection);
		addPropertiesFrom(this.fusion);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		DuplicateDetection duplicates = this.duplicateDetection.withInputs(module.getInput(0));
		Fusion fusedDuplicates = this.fusion.withInputs(duplicates);
		Difference nonDuplicates =
			new Difference().withInputs(module.getInput(0), new ArraySplit().withInputs(duplicates));
		module.getOutput(0).setInputs(new UnionAll().withInputs(fusedDuplicates, nonDuplicates));
	}
}
