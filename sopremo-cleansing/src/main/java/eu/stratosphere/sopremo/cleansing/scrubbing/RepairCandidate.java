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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.custom.CustomObjectNode;

/**
 * @author Arvid Heise
 */
public class RepairCandidate extends CustomObjectNode {
	private TextNode attribute;

	private IJsonNode correction;

	private Constraint violatedConstraint;

	public TextNode getAttribute() {
		return this.attribute;
	}

	public void setAttribute(TextNode attribute) {
		if (attribute == null)
			throw new NullPointerException("attribute must not be null");

		this.attribute = attribute;
	}

	public IJsonNode getCorrection() {
		return this.correction;
	}

	public void setCorrection(IJsonNode correction) {
		if (correction == null)
			throw new NullPointerException("correction must not be null");

		this.correction = correction;
	}

	public Constraint getViolatedConstraint() {
		return this.violatedConstraint;
	}

	public void setViolatedConstraint(Constraint violatedConstraint) {
		if (violatedConstraint == null)
			throw new NullPointerException("violatedConstraint must not be null");

		this.violatedConstraint = violatedConstraint;
	}

}
