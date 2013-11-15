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

import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.List;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.AbstractSopremoType;

/**
 * @author arv
 */
@DefaultSerializer(SpicyPathExpression.SpicyPathExpressionSerializer.class)
public class SpicyPathExpression extends AbstractSopremoType {
	public static class SpicyPathExpressionSerializer extends Serializer<SpicyPathExpression> {
		/*
		 * (non-Javadoc)
		 * @see com.esotericsoftware.kryo.Serializer#read(com.esotericsoftware.kryo.Kryo,
		 * com.esotericsoftware.kryo.io.Input, java.lang.Class)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public SpicyPathExpression read(Kryo kryo, Input input, Class<SpicyPathExpression> type) {
			return new SpicyPathExpression(new PathExpression((List<String>) kryo.readClassAndObject(input)));
		}

		/*
		 * (non-Javadoc)
		 * @see com.esotericsoftware.kryo.Serializer#write(com.esotericsoftware.kryo.Kryo,
		 * com.esotericsoftware.kryo.io.Output, java.lang.Object)
		 */
		@Override
		public void write(Kryo kryo, Output output, SpicyPathExpression object) {
			kryo.writeClassAndObject(output, object.getPathExpression().getPathSteps());
		}
		
		/* (non-Javadoc)
		 * @see com.esotericsoftware.kryo.Serializer#copy(com.esotericsoftware.kryo.Kryo, java.lang.Object)
		 */
		@Override
		public SpicyPathExpression copy(Kryo kryo, SpicyPathExpression original) {
			return new SpicyPathExpression(new PathExpression(original.getPathExpression()));
		}
	}

	private final PathExpression pathExpression;

	public SpicyPathExpression(PathExpression pathExpression) {
		this.pathExpression = pathExpression;
	}

	/**
	 * Initializes SpicyPathExpression.
	 */
	SpicyPathExpression() {
		this.pathExpression = null;
	}

	/**
	 * Initializes SpicyPathExpression.
	 * 
	 * @param targetNesting
	 * @param targetAttr
	 */
	public SpicyPathExpression(String... path) {
		this(new PathExpression(Lists.newArrayList(path)));
	}

	/**
	 * Returns the pathExpression.
	 * 
	 * @return the pathExpression
	 */
	public PathExpression getPathExpression() {
		return this.pathExpression;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.pathExpression.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.pathExpression.toString());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SpicyPathExpression other = (SpicyPathExpression) obj;
		return this.pathExpression.equals(other.pathExpression);
	}

	public String getFirstStep() {
		return this.pathExpression.getFirstStep();
	}

	public String getLastStep() {
		return this.pathExpression.getLastStep();
	}

}
