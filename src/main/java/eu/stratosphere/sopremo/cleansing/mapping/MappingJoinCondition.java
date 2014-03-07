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
package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.List;

import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.AbstractSopremoType;

/**
 * @author Arvid Heise, Tommy Neubert
 */
public class MappingJoinCondition extends AbstractSopremoType {
	private List<SpicyPathExpression> fromPaths;

	private List<SpicyPathExpression> toPaths;

	private boolean isMandatory;

	private boolean isMonodirectional;

	MappingJoinCondition() {

	}

	public MappingJoinCondition(List<SpicyPathExpression> fromPaths,
			List<SpicyPathExpression> toPaths, boolean isMandatory,
			boolean isMonodirectional) {
		this.fromPaths = fromPaths;
		this.toPaths = toPaths;
		this.isMandatory = isMandatory;
		this.isMonodirectional = isMonodirectional;
	}

	public MappingJoinCondition(JoinCondition condition) {
		this.fromPaths = MappingUtil.extractPathFrom(condition.getFromPaths());
		this.toPaths = MappingUtil.extractPathFrom(condition.getToPaths());
		this.isMandatory = condition.isMandatory();
		this.isMonodirectional = condition.isMonodirectional();
	}

	public List<SpicyPathExpression> getFromPaths() {
		return this.fromPaths;
	}

	public List<SpicyPathExpression> getToPaths() {
		return this.toPaths;
	}

	public boolean isMandatory() {
		return this.isMandatory;
	}

	public boolean isMonodirectional() {
		return this.isMonodirectional;
	}

	public JoinCondition generateSpicyType() {
		List<PathExpression> fromPaths = MappingUtil.createPaths(this.fromPaths);
		List<PathExpression> toPaths = MappingUtil.createPaths(this.toPaths);
		return new JoinCondition(fromPaths, toPaths, this.isMonodirectional,
			this.isMandatory);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingJoinCondition [");
		appendable.append("fromPaths=");
		append(appendable, this.fromPaths, ",");
		appendable.append(", ");
		appendable.append("toPaths=");
		append(appendable, this.toPaths, ",");
		appendable.append(", ");
		appendable.append("isMandatory=");
		TypeFormat.format(this.isMandatory, appendable);
		appendable.append(", isMonodirectional=");
		TypeFormat.format(this.isMonodirectional, appendable);
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fromPaths == null) ? 0 : fromPaths.hashCode());
		result = prime * result + (isMandatory ? 1231 : 1237);
		result = prime * result + (isMonodirectional ? 1231 : 1237);
		result = prime * result + ((toPaths == null) ? 0 : toPaths.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MappingJoinCondition other = (MappingJoinCondition) obj;
		if (fromPaths == null) {
			if (other.fromPaths != null)
				return false;
		} else if (!fromPaths.equals(other.fromPaths))
			return false;
		if (isMandatory != other.isMandatory)
			return false;
		if (isMonodirectional != other.isMonodirectional)
			return false;
		if (toPaths == null) {
			if (other.toPaths != null)
				return false;
		} else if (!toPaths.equals(other.toPaths))
			return false;
		return true;
	}


}
