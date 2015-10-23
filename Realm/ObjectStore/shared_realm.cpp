////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "shared_realm.hpp"

#include "realm_coordinator.hpp"
#include "realm_delegate.hpp"
#include "schema.hpp"
#include "transact_log_handler.hpp"

#include <realm/commit_log.hpp>
#include <realm/group_shared.hpp>

#include <mutex>

using namespace realm;
using namespace realm::_impl;

Realm::Config::Config(const Config& c)
: path(c.path)
, read_only(c.read_only)
, in_memory(c.in_memory)
, cache(c.cache)
, encryption_key(c.encryption_key)
, schema_version(c.schema_version)
, migration_function(c.migration_function)
{
    if (c.schema) {
        schema = std::make_unique<Schema>(*c.schema);
    }
}

Realm::Config::Config() = default;
Realm::Config::Config(Config&&) = default;
Realm::Config::~Config() = default;

Realm::Config& Realm::Config::operator=(realm::Realm::Config const& c)
{
    if (&c != this) {
        *this = Config(c);
    }
    return *this;
}

Realm::Realm(Config config)
: m_config(std::move(config))
{
    try {
        if (m_config.read_only) {
            m_read_only_group = std::make_unique<Group>(m_config.path, m_config.encryption_key.data(), Group::mode_ReadOnly);
            m_group = m_read_only_group.get();
        }
        else {
            m_history = realm::make_client_history(m_config.path, m_config.encryption_key.data());
            SharedGroup::DurabilityLevel durability = m_config.in_memory ? SharedGroup::durability_MemOnly :
                                                                           SharedGroup::durability_Full;
            m_shared_group = std::make_unique<SharedGroup>(*m_history, durability, m_config.encryption_key.data());
        }
    }
    catch (util::File::PermissionDenied const& ex) {
        throw RealmFileException(RealmFileException::Kind::PermissionDenied, "Unable to open a realm at path '" + m_config.path +
                             "'. Please use a path where your app has " + (m_config.read_only ? "read" : "read-write") + " permissions.");
    }
    catch (util::File::Exists const& ex) {
        throw RealmFileException(RealmFileException::Kind::Exists, "Unable to open a realm at path '" + m_config.path + "'");
    }
    catch (util::File::AccessError const& ex) {
        throw RealmFileException(RealmFileException::Kind::AccessError, "Unable to open a realm at path '" + m_config.path + "'");
    }
    catch (IncompatibleLockFile const&) {
        throw RealmFileException(RealmFileException::Kind::IncompatibleLockFile, "Realm file is currently open in another process "
        "which cannot share access with this process. All processes sharing a single file must be the same architecture.");
    }
}

void Realm::init(std::shared_ptr<RealmCoordinator> coordinator)
{
    m_coordinator = std::move(coordinator);

    // if there is an existing realm at the current path steal its schema/column mapping
    if (auto existing = m_coordinator->get_schema()) {
        m_config.schema = std::make_unique<Schema>(*existing);
        return;
    }

    try {
        // otherwise get the schema from the group
        auto target_schema = std::move(m_config.schema);
        auto target_schema_version = m_config.schema_version;
        m_config.schema_version = ObjectStore::get_schema_version(read_group());
        m_config.schema = std::make_unique<Schema>(ObjectStore::schema_from_group(read_group()));

        // if a target schema is supplied, verify that it matches or migrate to
        // it, as neeeded
        if (target_schema) {
            if (m_config.read_only) {
                if (m_config.schema_version == ObjectStore::NotVersioned) {
                    throw UnitializedRealmException("Can't open an un-initialized Realm without a Schema");
                }
                target_schema->validate();
                ObjectStore::verify_schema(*m_config.schema, *target_schema, true);
                m_config.schema = std::move(target_schema);
            }
            else {
                update_schema(std::move(target_schema), target_schema_version);
            }
        }
    }
    catch (...) {
        // Trying to unregister from the coordinator before we finish
        // construction will result in a deadlock
        m_coordinator = nullptr;
        throw;
    }
}

Realm::~Realm()
{
    if (m_coordinator) {
        m_coordinator->unregister_realm(this);
    }
}

Group *Realm::read_group()
{
    if (!m_group) {
        m_group = &const_cast<Group&>(m_shared_group->begin_read());
    }
    return m_group;
}

SharedRealm Realm::get_shared_realm(Config config)
{
    return RealmCoordinator::get_coordinator(config.path)->get_realm(config);
}

bool Realm::update_schema(std::unique_ptr<Schema> schema, uint64_t version)
{
    schema->validate();

    bool needs_update = !m_config.read_only && (m_config.schema_version != version || ObjectStore::needs_update(*m_config.schema, *schema));
    if (!needs_update) {
        ObjectStore::verify_schema(*m_config.schema, *schema, m_config.read_only);
        m_config.schema = std::move(schema);
        m_config.schema_version = version;
        return false;
    }

    // Store the old config/schema for the migration function, and update
    // our schema to the new one
    auto old_schema = std::move(m_config.schema);
    Config old_config(m_config);
    old_config.read_only = true;
    old_config.schema = std::move(old_schema);

    m_config.schema = std::move(schema);
    m_config.schema_version = version;

    auto migration_function = [&](Group*,  Schema&) {
        SharedRealm old_realm(new Realm(old_config));
        auto updated_realm = shared_from_this();
        m_config.migration_function(old_realm, updated_realm);
    };

    try {
        // update and migrate
        begin_transaction();
        bool changed = ObjectStore::update_realm_with_schema(read_group(), *old_config.schema,
                                                             version, *m_config.schema,
                                                             migration_function);
        commit_transaction();
        return changed;
    }
    catch (...) {
        if (is_in_transaction()) {
            cancel_transaction();
        }
        m_config.schema_version = old_config.schema_version;
        m_config.schema = std::move(old_config.schema);
        throw;
    }
}

static void check_read_write(Realm *realm)
{
    if (realm->config().read_only) {
        throw InvalidTransactionException("Can't perform transactions on read-only Realms.");
    }
}

void Realm::verify_thread() const
{
    if (m_thread_id != std::this_thread::get_id()) {
        throw IncorrectThreadException("Realm accessed from incorrect thread.");
    }
}

void Realm::begin_transaction()
{
    check_read_write(this);
    verify_thread();

    if (m_in_transaction) {
        throw InvalidTransactionException("The Realm is already in a write transaction");
    }

    // make sure we have a read transaction
    read_group();

    transaction::begin(*m_shared_group, *m_history, m_delegate.get());
    m_in_transaction = true;
}

void Realm::commit_transaction()
{
    check_read_write(this);
    verify_thread();

    if (!m_in_transaction) {
        throw InvalidTransactionException("Can't commit a non-existing write transaction");
    }

    m_in_transaction = false;
    transaction::commit(*m_shared_group, *m_history, m_delegate.get());
    m_coordinator->send_commit_notifications();
}

void Realm::cancel_transaction()
{
    check_read_write(this);
    verify_thread();

    if (!m_in_transaction) {
        throw InvalidTransactionException("Can't cancel a non-existing write transaction");
    }

    m_in_transaction = false;
    transaction::cancel(*m_shared_group, *m_history, m_delegate.get());
}

void Realm::invalidate()
{
    verify_thread();
    check_read_write(this);

    if (m_in_transaction) {
        cancel_transaction();
    }
    if (!m_group) {
        return;
    }

    m_shared_group->end_read();
    m_group = nullptr;
}

bool Realm::compact()
{
    verify_thread();

    if (m_config.read_only) {
        throw InvalidTransactionException("Can't compact a read-only Realm");
    }
    if (m_in_transaction) {
        throw InvalidTransactionException("Can't compact a Realm within a write transaction");
    }

    Group* group = read_group();
    for (auto &object_schema : *m_config.schema) {
        ObjectStore::table_for_object_type(group, object_schema.name)->optimize();
    }
    m_shared_group->end_read();
    m_group = nullptr;

    return m_shared_group->compact();
}

void Realm::notify()
{
    verify_thread();

    if (m_shared_group->has_changed()) { // Throws
        if (m_delegate) {
            m_delegate->changes_available();
        }
        if (m_auto_refresh) {
            if (m_group) {
                transaction::advance(*m_shared_group, *m_history, m_delegate.get());
            }
            else if (m_delegate) {
                m_delegate->did_change({}, {});
            }
        }
    }
}

bool Realm::refresh()
{
    verify_thread();
    check_read_write(this);

    // can't be any new changes if we're in a write transaction
    if (m_in_transaction) {
        return false;
    }

    // advance transaction if database has changed
    if (!m_shared_group->has_changed()) { // Throws
        return false;
    }

    if (m_group) {
        transaction::advance(*m_shared_group, *m_history, m_delegate.get());
    }
    else {
        // Create the read transaction
        read_group();
    }

    return true;
}

uint64_t Realm::get_schema_version(const realm::Realm::Config &config)
{
    auto coordinator = RealmCoordinator::get_existing_coordinator(config.path);
    if (coordinator) {
        return coordinator->get_schema_version();
    }

    return ObjectStore::get_schema_version(Realm(config).read_group());
}
