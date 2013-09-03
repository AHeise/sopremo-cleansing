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

import java.io.IOException;

import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.function.SopremoFunction2;
import eu.stratosphere.sopremo.function.SopremoVarargFunction;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
public class StringUDFs implements BuiltinProvider {

	public static final NORMALIZE_WHITESPACES NORMALIZE_WHITESPACES = new NORMALIZE_WHITESPACES();
	public static final REMOVE_ALL_CHARACTERS REMOVE_ALL_CHARACTERS = new REMOVE_ALL_CHARACTERS();
	public static final REMOVE_ALL_STRINGS REMOVE_ALL_STRINGS = new REMOVE_ALL_STRINGS();
	public static final CONCAT_STRINGS CONCAT_STRINGS = new CONCAT_STRINGS();
	public static final LOWER_CASE LOWER_CASE = new LOWER_CASE();
	public static final UPPER_CASE UPPER_CASE = new UPPER_CASE();

	/**
	 * This function normalizes a given value by removing all unneccessary
	 * whitespaces. This means that all leading and trailing whitespaces are
	 * removed and all "multiple" whitespaces inbetween are replaced by a single
	 * whitespace. The usage of this function is shown in the following example:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$fullname_normalized: normalize_whitespaces($fullname),
	 * 	...
	 * </pre></code>
	 * 
	 * If you want to use that function inside a script make sure that the given
	 * parameter (in the example: <code>$fullname</code>) references to a
	 * textual value.
	 */
	@Name(noun = "normalize_whitespaces")
	public static class NORMALIZE_WHITESPACES extends
			SopremoFunction1<TextNode> {

		public NORMALIZE_WHITESPACES() {
			super("normalize_whitespaces");
		}

		private String DOUBLE_WHITESPACE = "  ";

		private String SINGLE_WHITESPACE = " ";

		@Override
		protected IJsonNode call(TextNode inputNode) {
			String input = inputNode.toString();
			input = input.trim();
			while (input.contains(this.DOUBLE_WHITESPACE)) {
				input = input.replaceAll(this.DOUBLE_WHITESPACE,
						this.SINGLE_WHITESPACE);
			}
			return TextNode.valueOf(input);
		}

	};

	/**
	 * This function removes all occurrences of the given characters (removals)
	 * inside a specified text-value (input). Although the removals are given as
	 * a whole string, each character is processed individually. To remove whole
	 * character-sequences please use {@link StringUDFs#REMOVE_ALL_STRINGS}. The
	 * following example shows the usage of this function inside of a
	 * meteor-script:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$firstname_cleaned: remove_all_characters($firstname, "\'()[]"),
	 * 	...
	 * </pre></code> This code snippet will remove all occurrences of: ' ( ) [
	 * and ] inside of <code>$firstname</code>. When using this function please
	 * make sure the first parameter references a textual value.
	 */
	@Name(noun = "remove_all_characters")
	public static class REMOVE_ALL_CHARACTERS extends
			SopremoFunction2<TextNode, TextNode> {

		public REMOVE_ALL_CHARACTERS() {
			super("remove_all_characters");
		}

		private String REPLACE_CHAR = "";

		@Override
		protected IJsonNode call(TextNode inputNode, TextNode removals) {
			String input = inputNode.toString();
			String removalString = removals.toString();

			for (Character c : removalString.toCharArray()) {
				input = input.replace(c.toString(), this.REPLACE_CHAR);
			}

			return TextNode.valueOf(input);
		}

	};

	/**
	 * This function works like {@link StringUDFs#REMOVE_ALL_CHARACTERS}, but in
	 * addition to that, it is able to remove whole character-sequences. The
	 * following example shows the usage of this function:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$firstname_cleaned: remove_all_strings($firstname, ["invalid", "test"]),
	 * 	...
	 * </pre></code> Each string inside the given array will be removed from the
	 * first parameter. Even when removing only one string, the array brackets
	 * must be used when calling this function.
	 */
	@Name(noun = "remove_all_strings")
	public static class REMOVE_ALL_STRINGS extends
			SopremoFunction2<TextNode, ArrayNode<TextNode>> {

		public REMOVE_ALL_STRINGS() {
			super("remove_all_strings");
		}

		private String REPLACE_CHAR = "";

		@Override
		protected IJsonNode call(TextNode inputNode,
				ArrayNode<TextNode> removals) {
			String input = inputNode.toString();

			for (TextNode removalString : removals) {
				input = input.replace(removalString.toString(),
						this.REPLACE_CHAR);
			}

			return TextNode.valueOf(input);
		}

	};

	/**
	 * This function concatenates several textual values to a single one. This
	 * function has no limitations to the number of parameters, which means you
	 * can concatenate a variable number of text-values with one function call.
	 * The following example shows the usage of this function in a script:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$fullname: concat_strings($firstname, " ", $lastname, ", ", $title),
	 * 	...
	 * </pre></code>In the case of: <br/>
	 * <code>
	 * 	$firstname = "Max" <br/>
	 * 	$lastname = "Mustermann" <br/>
	 * 	$titles = "Dr." <br/>
	 * </code> a call of this function will lead to a fullname of
	 * "Max Mustermann, Dr.". Please make sure that all given parameters
	 * reference a textual value.
	 */
	@Name(noun = "concat_strings")
	public static class CONCAT_STRINGS extends SopremoVarargFunction {

		public CONCAT_STRINGS() {
			super("concat_strings", 0);
		}

		@Override
		public IJsonNode call(IArrayNode<IJsonNode> params) {
			StringBuilder builder = new StringBuilder();

			for (IJsonNode node : params) {
				if (node instanceof TextNode) {
					try {
						((TextNode) node).appendAsString(builder);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			return TextNode.valueOf(builder.toString());
		}

		@SuppressWarnings({ "unchecked", "unused" })
		private void concatArrayContent(IArrayNode<IJsonNode> arrayNode,
				StringBuilder builder) {
			for (IJsonNode node : arrayNode) {
				if (node instanceof IArrayNode) {
					this.concatArrayContent((IArrayNode<IJsonNode>) node,
							builder);
				} else {
					// builder.append(((TextNode) node).getTextValue());
					try {
						((TextNode) node).appendAsString(builder);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	};

	/**
	 * This function converts a textual value to there respective
	 * lower-case-representation. The following example shows the usage of this
	 * function inside a meteor-script:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$fullname_lc: lower_case($fullname),
	 * 	...
	 * </pre></code> A call of this function with the parameter "mAX MusTeRMAnN"
	 * will lead to "max mustermann". When using this function please make sure
	 * the given parameter references a textual value.
	 */
	@Name(noun = "lower_case")
	public static class LOWER_CASE extends SopremoFunction1<TextNode> {

		public LOWER_CASE() {
			super("lower_case");
		}

		@Override
		protected IJsonNode call(TextNode inputString) {
			return TextNode.valueOf(inputString.toString().toLowerCase());
		}
	};

	/**
	 * This function converts a textual value to there respective
	 * upper-case-representation. The following example shows the usage of this
	 * function inside a meteor-script:
	 * 
	 * <code><pre>
	 * 	...
	 * 	$fullname_uc: upper_case($fullname),
	 * 	...
	 * </pre></code> A call of this function with the parameter "mAX MusTeRMAnN"
	 * will lead to "MAX MUSTERMANN". When using this function please make sure
	 * the given parameter references a textual value.
	 */
	@Name(noun = "upper_case")
	public static class UPPER_CASE extends SopremoFunction1<TextNode> {

		public UPPER_CASE() {
			super("upper_case");
		}

		@Override
		protected IJsonNode call(TextNode inputString) {
			return TextNode.valueOf(inputString.toString().toUpperCase());
		}
	};
}