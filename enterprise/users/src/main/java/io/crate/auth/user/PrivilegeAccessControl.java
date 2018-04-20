/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import io.crate.analyze.user.Privilege;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.metadata.RelationName;

public class PrivilegeAccessControl implements AccessControl {

    @Override
    public void raiseUnknownIfInvisible(User user, Privilege.Clazz clazz, String ident) {
        if (!user.hasAnyPrivilege(clazz, ident)) {
            switch (clazz) {
                case CLUSTER:
                    throw new MissingPrivilegeException(user.name());

                case SCHEMA:
                    throw new SchemaUnknownException(ident);

                case TABLE:
                case VIEW:
                    throw new RelationUnknown(RelationName.fromIndexName(ident));

                default:
                    throw new AssertionError("Invalid clazz: " + clazz);
            }
        }
    }

    @Override
    public void ensureHasPrivilegesForTable(User user, RelationName relationName) {

    }

    @Override
    public void ensureHasPrivilegesForView(User user, RelationName relationName) {

    }
}
