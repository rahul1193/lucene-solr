/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeError;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

/**
 * @author rahulanishetty
 * @since 12/07/18.
 */
public class NearQueryNode extends QueryNodeImpl implements FieldableNode {

  private int value = 0;
  private final boolean ordered;

  /**
   * @param clauses - QueryNode Tree with the phrase
   * @param value   - slop value
   */
  public NearQueryNode(List<QueryNode> clauses, int value, boolean ordered) {
    if (clauses == null) {
      throw new QueryNodeError(new MessageImpl(
          QueryParserMessages.NODE_ACTION_NOT_SUPPORTED, "query", "null"));
    }
    this.value = value;
    this.ordered = ordered;
    setLeaf(false);
    allocate();
    add(clauses);
  }

  public QueryNode getChild() {
    return getChildren().get(0);
  }

  public int getValue() {
    return this.value;
  }

  public CharSequence getValueString() {
    Float f = Float.valueOf(this.value);
    if (f == f.longValue())
      return "" + f.longValue();
    else
      return "" + f;

  }

  public boolean isOrdered() {
    return ordered;
  }

  @Override
  public String toString() {
    if (getChildren() == null || getChildren().size() == 0)
      return "<boolean operation='" + (ordered ? "onear" : "near") + "'/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<boolean operation='").append(ordered ? "onear" : "near").append("'>");
    for (QueryNode child : getChildren()) {
      sb.append("\n");
      sb.append(child.toString());

    }
    sb.append("\n</boolean>");
    return "<slop value='" + getValueString() + "'>" + "\n" +
        sb.toString() + "\n</slop>";
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (getChild() == null)
      return "";
    return getChild().toQueryString(escapeSyntaxParser) + "/"
        + getValueString();
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    NearQueryNode clone = (NearQueryNode) super.cloneTree();

    clone.value = this.value;

    return clone;
  }

  @Override
  public CharSequence getField() {
    QueryNode child = getChild();

    if (child instanceof FieldableNode) {
      return ((FieldableNode) child).getField();
    }

    return null;

  }

  @Override
  public void setField(CharSequence fieldName) {
    QueryNode child = getChild();

    if (child instanceof FieldableNode) {
      ((FieldableNode) child).setField(fieldName);
    }

  }
}

