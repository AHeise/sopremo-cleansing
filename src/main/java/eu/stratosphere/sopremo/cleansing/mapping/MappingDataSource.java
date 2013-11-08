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

import it.unibas.spicy.model.datasource.DataSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;

public class MappingDataSource extends AbstractSopremoType {

	private List<MappingKeyConstraint> keyConstraints = new ArrayList<MappingKeyConstraint>();

	private MappingSchema targetSchema;

	MappingDataSource() {

	}

	/**
	 * @param targetKey
	 */
	public void addKeyConstraint(MappingKeyConstraint targetKey) {
		this.keyConstraints.add(targetKey);
	}

	public MappingSchema getTargetSchema() {
		return this.targetSchema;
	}

	public void setTargetSchema(MappingSchema targetSchema) {
		if (targetSchema == null)
			throw new NullPointerException("targetSchema must not be null");

		this.targetSchema = targetSchema;
	}

	public DataSource generateSpicyType() {
		DataSource dataSource = new DataSource(EntityMapping.type, this.targetSchema.generateSpicyType());
		for (MappingKeyConstraint keyConstraint : this.keyConstraints) {
			dataSource.addKeyConstraint(keyConstraint.generateSpicyType());
		}
		return dataSource;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.keyConstraints.hashCode();
		result = prime * result + this.targetSchema.hashCode();
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
		MappingDataSource other = (MappingDataSource) obj;
		return this.targetSchema.equals(other.targetSchema) && this.keyConstraints.equals(other.keyConstraints);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingDataSource [keyConstraints=");
		append(appendable, this.keyConstraints, ",");
		appendable.append(", targetSchema=");
		this.targetSchema.appendAsString(appendable);
		appendable.append("]");
	}

}
