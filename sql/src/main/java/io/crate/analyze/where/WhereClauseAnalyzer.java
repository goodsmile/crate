/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze.where;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.operators.SubQueryAndParamBinder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WhereClauseAnalyzer {

    /**
     * Replace parameters and sub-queries with the related values and analyze the query afterwards.
     */
    public static WhereClause bindAndAnalyze(WhereClause where,
                                             Row params,
                                             Map<SelectSymbol, Object> subQueryValues,
                                             AbstractTableRelation tableRelation,
                                             Functions functions,
                                             TransactionContext transactionContext) {
        if (where.hasQuery()) {
            Symbol query = SubQueryAndParamBinder.convert(where.query(), params, subQueryValues);
            if (tableRelation instanceof DocTableRelation) {
                WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, (DocTableRelation) tableRelation);
                return whereClauseAnalyzer.analyze(query, transactionContext);
            } else {
                return new WhereClause(query);
            }
        }
        return where;
    }

    private final Functions functions;
    private final DocTableInfo table;
    private final EqualityExtractor eqExtractor;
    private final EvaluatingNormalizer normalizer;

    public WhereClauseAnalyzer(Functions functions, DocTableRelation tableRelation) {
        this.functions = functions;
        this.table = tableRelation.tableInfo();
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, tableRelation);
        this.eqExtractor = new EqualityExtractor(normalizer);
    }

    public WhereClause analyze(Symbol query, TransactionContext transactionContext) {
        if (query.equals(Literal.BOOLEAN_TRUE)) {
            return WhereClause.MATCH_ALL;
        }
        WhereClauseValidator.validate(query);
        Symbol queryGenColsProcessed = GeneratedColumnExpander.maybeExpand(
            query,
            table.generatedColumns(),
            Lists2.concat(table.partitionedByColumns(), Lists2.copyAndReplace(table.primaryKey(), table::getReference))
        );
        if (!query.equals(queryGenColsProcessed)) {
            query = normalizer.normalize(queryGenColsProcessed, transactionContext);
        }

        List<ColumnIdent> pkCols;
        boolean versionInQuery = Symbols.containsColumn(query, DocSysColumns.VERSION);
        if (versionInQuery) {
            pkCols = new ArrayList<>(table.primaryKey().size() + 1);
            pkCols.addAll(table.primaryKey());
            pkCols.add(DocSysColumns.VERSION);
        } else {
            pkCols = table.primaryKey();
        }
        List<List<Symbol>> pkValues = eqExtractor.extractExactMatches(pkCols, query, transactionContext);
        Set<Symbol> clusteredBy = Collections.emptySet();
        DocKeys docKeys = null;
        if (!pkCols.isEmpty() && pkValues != null) {
            int clusterdIdx = -1;
            if (table.clusteredBy() != null) {
                clusterdIdx = table.primaryKey().indexOf(table.clusteredBy());
                if (clusterdIdx >= 0) {
                    clusteredBy = new HashSet<>(pkValues.size());
                    for (List<Symbol> row : pkValues) {
                        clusteredBy.add(row.get(clusterdIdx));
                    }
                }
            }
            List<Integer> partitionsIdx = null;
            if (table.isPartitioned()) {
                partitionsIdx = WhereClauseOptimizer.getPartitionIndices(table.primaryKey(), table.partitionedBy());
            }
            docKeys = new DocKeys(pkValues, versionInQuery, clusterdIdx, partitionsIdx);
        } else {
            clusteredBy = getClusteredByLiterals(query, eqExtractor, transactionContext);
        }

        List<String> partitions = null;
        if (table.isPartitioned() && docKeys == null) {
            if (table.partitions().isEmpty()) {
                return WhereClause.NO_MATCH;
            }
            ResolvedPartitions resolvedPartitions =
                PartitionResolver.resolvePartitions(query, table, functions, transactionContext);
            partitions = resolvedPartitions.partitions;
            query = resolvedPartitions.query;
        }
        return new WhereClause(query, docKeys, partitions, clusteredBy);
    }

    private Set<Symbol> getClusteredByLiterals(Symbol query, EqualityExtractor ee, TransactionContext transactionContext) {
        if (table.clusteredBy() != null) {
            List<List<Symbol>> clusteredValues = ee.extractParentMatches(
                ImmutableList.of(table.clusteredBy()), query, transactionContext);
            if (clusteredValues != null) {
                Set<Symbol> clusteredBy = new HashSet<>(clusteredValues.size());
                for (List<Symbol> row : clusteredValues) {
                    clusteredBy.add(row.get(0));
                }
                return clusteredBy;
            }
        }
        return Collections.emptySet();
    }


}
