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
package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.io.IOException;

import com.esotericsoftware.kryo.DefaultSerializer;

import eu.stratosphere.sopremo.SingletonSerializer;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.UnevaluableSingletonExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.Immutable;

/**
 * 
 */
public class PairFilter extends BooleanExpression {
	/**
	 * 
	 */
	private static final UnevaluableSingletonExpression UNKNOWN = new UnknownExpression();

	@DefaultSerializer(UnknownSerializer.class)
	@Immutable
	private static final class UnknownExpression extends UnevaluableSingletonExpression {
		/**
		 * Initializes PairFilter.UnknownExpression.
		 */
		public UnknownExpression() {
			super("unknown");
		}
	};

	public static final class UnknownSerializer extends SingletonSerializer {
		/**
		 * Initializes PairFilter.UnknownSerializer.
		 */
		public UnknownSerializer() {
			super(UNKNOWN);
		}
	}

	private EvaluationExpression leftIdProjection = UNKNOWN, rightIdProjection = UNKNOWN;

	/**
	 * Returns the value of idProjection.
	 * 
	 * @return the idProjection
	 */
	public EvaluationExpression getIdProjection() {
		if (this.leftIdProjection == this.rightIdProjection)
			return this.leftIdProjection;
		throw new IllegalStateException("May only return id projection if the same for both sources");
	}

	/**
	 * Sets the value of idProjection to the given value.
	 * 
	 * @param idProjection
	 *        the idProjection to set
	 */
	public void setIdProjection(EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.leftIdProjection = this.rightIdProjection = idProjection;
	}

	/**
	 * Sets the value of idProjection to the given value.
	 * 
	 * @param idProjection
	 *        the idProjection to set
	 * @return this
	 */
	public PairFilter withIdProjection(EvaluationExpression idProjection) {
		this.setIdProjection(idProjection);
		return this;
	}

	/**
	 * Returns the value of leftIdProjection.
	 * 
	 * @return the leftIdProjection
	 */
	public EvaluationExpression getLeftIdProjection() {
		return this.leftIdProjection;
	}

	/**
	 * Sets the value of leftIdProjection to the given value.
	 * 
	 * @param leftIdProjection
	 *        the leftIdProjection to set
	 */
	public void setLeftIdProjection(EvaluationExpression leftIdProjection) {
		if (leftIdProjection == null)
			throw new NullPointerException("leftIdProjection must not be null");

		this.leftIdProjection = leftIdProjection;
	}

	/**
	 * Returns the value of rightIdProjection.
	 * 
	 * @return the rightIdProjection
	 */
	public EvaluationExpression getRightIdProjection() {
		return this.rightIdProjection;
	}

	/**
	 * Sets the value of rightIdProjection to the given value.
	 * 
	 * @param rightIdProjection
	 *        the rightIdProjection to set
	 */
	public void setRightIdProjection(EvaluationExpression rightIdProjection) {
		if (rightIdProjection == null)
			throw new NullPointerException("rightIdProjection must not be null");

		this.rightIdProjection = rightIdProjection;
	}

	public boolean isConfigured() {
		return this.leftIdProjection != UNKNOWN && this.rightIdProjection != UNKNOWN;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.leftIdProjection.hashCode();
		result = prime * result + this.rightIdProjection.hashCode();
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
		PairFilter other = (PairFilter) obj;
		return this.leftIdProjection.equals(other.leftIdProjection) &&
			this.rightIdProjection.equals(other.rightIdProjection);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable,
			"PairFilter [", "leftIdProjection=", this.leftIdProjection, ", ",
			"rightIdProjection=", this.rightIdProjection, "]");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.BooleanExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public BooleanNode evaluate(IJsonNode pair) {
		@SuppressWarnings("unchecked")
		final IArrayNode<IJsonNode> array = (IArrayNode<IJsonNode>) pair;
		return BooleanNode.valueOf(evaluate(array.get(0), array.get(1)));
	}

	public boolean evaluate(final IJsonNode left, final IJsonNode right) {
		return this.leftIdProjection.evaluate(left).compareTo(this.rightIdProjection.evaluate(right)) < 0;
	}

}
