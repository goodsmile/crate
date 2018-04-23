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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.auth.user.AccessControl;
import io.crate.auth.user.User;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewsMetaData;
import io.crate.metadata.view.ViewsMetaDataTest;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemasTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    public ClusterService clusterService;

    @Mock
    public ClusterState clusterState;

    @Mock
    public MetaData metaData;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.getConcreteAllOpenIndices()).thenReturn(new String[0]);
        when(metaData.templates()).thenReturn(ImmutableOpenMap.of());
    }

    @Test
    public void testSystemSchemaIsNotWritable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"foo.bar\" doesn't support or allow INSERT " +
                                        "operations, as it is read-only.");

        RelationName relationName = new RelationName("foo", "bar");
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.ident()).thenReturn(relationName);
        when(tableInfo.supportedOperations()).thenReturn(Operation.SYS_READ_ONLY);
        when(schemaInfo.getTableInfo(relationName.name())).thenReturn(tableInfo);
        when(schemaInfo.name()).thenReturn(relationName.schema());

        Schemas schemas = getReferenceInfos(schemaInfo);
        schemas.getTableInfo(User.CRATE_USER, relationName, Operation.INSERT);
    }

    @Test
    public void testTableAliasIsNotWritable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"foo.bar\" doesn't support or allow INSERT operations.");

        RelationName relationName = new RelationName("foo", "bar");
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        DocTableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.ident()).thenReturn(relationName);
        when(schemaInfo.getTableInfo(relationName.name())).thenReturn(tableInfo);
        when(schemaInfo.name()).thenReturn(relationName.schema());
        when(tableInfo.isAlias()).thenReturn(true);


        Schemas schemas = getReferenceInfos(schemaInfo);
        schemas.getTableInfo(User.CRATE_USER, relationName, Operation.INSERT);
    }

    @Test
    public void testSchemasFromUDF() {
        MetaData metaData = MetaData.builder()
            .putCustom(
                UserDefinedFunctionsMetaData.TYPE,
                UserDefinedFunctionsMetaData.of(
                    new UserDefinedFunctionMetaData("new_schema", "my_function", ImmutableList.of(), DataTypes.STRING,
                        "burlesque", "Hello, World!Q")
                )
            ).build();
        assertThat(Schemas.getNewCurrentSchemas(metaData), containsInAnyOrder("doc", "new_schema"));
    }

    @Test
    public void testSchemasFromViews() {
        MetaData metaData = MetaData.builder()
            .putCustom(
                ViewsMetaData.TYPE,
                ViewsMetaDataTest.createMetaData()
            ).build();
        assertThat(Schemas.getNewCurrentSchemas(metaData), containsInAnyOrder("doc", "my_schema"));
    }


    @Test
    public void testCurrentSchemas() throws Exception {
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("doc.d1")
                .state(IndexMetaData.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetaData.builder("doc.d2")
                .state(IndexMetaData.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetaData.builder("foo.f1")
                .state(IndexMetaData.State.CLOSE)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .put(IndexMetaData.builder("foo.f2")
                .state(IndexMetaData.State.OPEN)
                .settings(Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_VERSION_CREATED, Version.CURRENT))
                .build(), true)
            .build();
        assertThat(Schemas.getNewCurrentSchemas(metaData), contains("foo", "doc"));
    }

    private Schemas getReferenceInfos(SchemaInfo schemaInfo) {
        Map<String, SchemaInfo> builtInSchema = new HashMap<>();
        builtInSchema.put(schemaInfo.name(), schemaInfo);
        return new Schemas(
            Settings.EMPTY, AccessControl.ALLOW_ALL, builtInSchema, clusterService, mock(DocSchemaInfoFactory.class));
    }
}
