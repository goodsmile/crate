/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.collect.Sets;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.analyze.symbol.*;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class Rewriter {

    /**
     * Rewrite an Outer join to an inner join if possible.
     * <p>
     * Conditions on OUTER tables are not pushed down when a MultiSourceSelect is initially created because
     * the whereClause needs to be checked AFTER the join
     * (because if the join generates NULL rows the whereClause could become TRUE on those NULL rows)
     * <p>
     * See the following two examples where <code>t2</code> is the OUTER table:
     * <p>
     * <pre>
     *     select * from t1
     *          left join t2 on t1.t2_id = t2.id
     *     where
     *          coalesce(t2.x, 20) > 10   # becomes TRUE for null rows
     * </pre>
     * <p>
     * <p>
     * <p>
     * But if we know that the whereClause cannot possible become TRUE then we can push it down
     * (re-writing this into an inner join)
     * <p>
     * This is possible because all rows that are generated by the left-join would be removed again by the whereClause anyway.
     * <p>
     * <pre>
     *     select * from t1
     *          left join t2 on t1.t2_id = t2.id
     *     where
     *          t2.x > 10   # if t2.x is NULL this is always FALSE
     * </pre>
     */
    public static void tryRewriteOuterToInnerJoin(EvaluatingNormalizer normalizer,
                                                  JoinPair joinPair,
                                                  QuerySpec multiSourceQuerySpec,
                                                  QualifiedName left,
                                                  QualifiedName right,
                                                  QuerySpec leftQuerySpec,
                                                  QuerySpec rightQuerySpec) {
        assert left.equals(joinPair.left()) : "This JoinPair has a different left Qualified name: " + joinPair.left();
        assert right.equals(joinPair.right()) :
            "This JoinPair has a different left Qualified name: " + joinPair.right();

        JoinType joinType = joinPair.joinType();
        if (!joinType.isOuter()) {
            return;
        }
        WhereClause where = multiSourceQuerySpec.where();
        if (!where.hasQuery()) {
            return;
        }
        final Map<QualifiedName, QuerySpec> outerRelations = new HashMap<>(2);
        switch (joinType) {
            case LEFT:
                outerRelations.put(right, rightQuerySpec);
                break;
            case RIGHT:
                outerRelations.put(left, leftQuerySpec);
                break;
            case FULL:
                outerRelations.put(left, leftQuerySpec);
                outerRelations.put(right, rightQuerySpec);
                break;
        }

        Map<Set<QualifiedName>, Symbol> splitQueries = QuerySplitter.split(where.query());
        for (QualifiedName outerRelation : outerRelations.keySet()) {
            Symbol outerRelationQuery = splitQueries.remove(Sets.newHashSet(outerRelation));
            if (outerRelationQuery != null) {
                QuerySpec outerSpec = outerRelations.get(outerRelation);

                Symbol symbol = Symbols.replaceField(
                    outerRelationQuery,
                    new java.util.function.Function<Field, Symbol>() {
                        @Nullable
                        @Override
                        public Symbol apply(@Nullable Field input) {
                            if (input != null && input.relation().getQualifiedName().equals(outerRelation)) {
                                return Literal.NULL;
                            }
                            return input;
                        }
                    }
                );
                Symbol normalized = normalizer.normalize(symbol, null);
                if (WhereClause.canMatch(normalized)) {
                    splitQueries.put(Sets.newHashSet(outerRelation), outerRelationQuery);
                } else {
                    applyOuterJoinRewrite(
                        joinPair,
                        multiSourceQuerySpec,
                        outerSpec,
                        outerRelation,
                        splitQueries,
                        outerRelationQuery
                    );
                }
            }
        }
    }

    private static void applyOuterJoinRewrite(JoinPair joinPair,
                                              QuerySpec multiSourceQuerySpec,
                                              QuerySpec outerSpec,
                                              QualifiedName outerRelation,
                                              Map<Set<QualifiedName>, Symbol> splitQueries,
                                              Symbol outerRelationQuery) {
        RemoveFieldsNotToCollectFunction removeFieldsNotToCollectFunction =
            new RemoveFieldsNotToCollectFunction(outerRelation, multiSourceQuerySpec.outputs(), joinPair.condition());
        outerSpec.where(outerSpec.where().add(Symbols.replaceField(
            outerRelationQuery,
            removeFieldsNotToCollectFunction)));
        if (splitQueries.isEmpty()) { // All queries where successfully pushed down
            joinPair.joinType(JoinType.INNER);
            multiSourceQuerySpec.where(WhereClause.MATCH_ALL);
        } else { // Query only for one relation was pushed down
            if (joinPair.left().equals(outerRelation)) {
                joinPair.joinType(JoinType.LEFT);
            } else {
                joinPair.joinType(JoinType.RIGHT);
            }
            multiSourceQuerySpec.where(new WhereClause(AndOperator.join(splitQueries.values())));
        }
        for (Field fieldToRemove : removeFieldsNotToCollectFunction.fieldsToNotCollect()) {
            outerSpec.outputs().remove(fieldToRemove);
            multiSourceQuerySpec.outputs().remove(fieldToRemove);
        }
    }

    /**
     * Remove fields which are being replaced and no longer
     * required to be collected and add them in {@link #fieldsToNotCollect()}
     */
    private static class RemoveFieldsNotToCollectFunction implements Function<Field, Symbol> {
        private final QualifiedName outerRelation;
        private final List<Symbol> mssOutputSymbols;
        private final Symbol joinCondition;
        private final Set<Field> fieldsToNotCollect;

        RemoveFieldsNotToCollectFunction(QualifiedName outerRelation,
                                         List<Symbol> mssOutputSymbols,
                                         Symbol joinCondition) {
            this.outerRelation = outerRelation;
            this.mssOutputSymbols = mssOutputSymbols;
            this.joinCondition = joinCondition;
            this.fieldsToNotCollect = new HashSet<>();
        }

        @Nullable
        @Override
        public Symbol apply(@Nullable Field input) {
            if (input == null) {
                return null;
            }
            if (!input.relation().getQualifiedName().equals(outerRelation)) {
                return input;
            }

            // if the column was only added to the outerSpec outputs because of the whereClause
            // it's possible to not collect it as long is it isn't used somewhere else
            if (!mssOutputSymbols.contains(input) &&
                !SymbolVisitors.any(symbol -> Objects.equals(input, symbol), joinCondition)) {
                fieldsToNotCollect.add(input);
            }
            return input;
        }

        Collection<Field> fieldsToNotCollect() {
            return fieldsToNotCollect;
        }
    }
}
