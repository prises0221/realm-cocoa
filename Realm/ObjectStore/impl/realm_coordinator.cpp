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

#include "realm_coordinator.hpp"

#include "external_commit_helper.hpp"

using namespace realm;
using namespace realm::_impl;

static std::mutex s_coordinator_mutex;
static std::map<std::string, std::weak_ptr<RealmCoordinator>> s_coordinators_per_path;

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    std::shared_ptr<RealmCoordinator> coordinator;

    auto it = s_coordinators_per_path.find(path);
    if (it != s_coordinators_per_path.end()) {
        coordinator = it->second.lock();
    }

    if (!coordinator) {
        s_coordinators_per_path[path] = coordinator = std::make_shared<RealmCoordinator>();
    }

    return coordinator;
}

std::shared_ptr<RealmCoordinator> RealmCoordinator::get_existing_coordinator(StringData path)
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    auto it = s_coordinators_per_path.find(path);
    return it == s_coordinators_per_path.end() ? nullptr : it->second.lock();
}

std::shared_ptr<Realm> RealmCoordinator::get_realm(Realm::Config config)
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    if (!m_notifier) {
        m_config = config;
        m_notifier = std::make_unique<ExternalCommitHelper>(config.path);
    }
    else {
        if (m_config.read_only != config.read_only) {
            throw MismatchedConfigException("Realm at path already opened with different read permissions.");
        }
        if (m_config.in_memory != config.in_memory) {
            throw MismatchedConfigException("Realm at path already opened with different inMemory settings.");
        }
        if (m_config.encryption_key != config.encryption_key) {
            throw MismatchedConfigException("Realm at path already opened with a different encryption key.");
        }
        if (m_config.schema_version != config.schema_version && config.schema_version != ObjectStore::NotVersioned) {
            throw MismatchedConfigException("Realm at path already opened with different schema version.");
        }
        // FIXME - enable schma comparison
        if (/* DISABLES CODE */ (false) && m_config.schema != config.schema) {
            throw MismatchedConfigException("Realm at path already opened with different schema");
        }
        // FIXME: wat?
        m_config.migration_function = config.migration_function;
    }

    auto thread_id = std::this_thread::get_id();
    if (config.cache) {
        for (auto& weakRealm : m_cached_realms) {
            // can be null if we jumped in between ref count hitting zero and
            // unregister_realm() getting the lock
            if (auto realm = weakRealm.lock()) {
                if (realm->thread_id() == thread_id) {
                    return realm;
                }
            }
        }
    }

    auto realm = std::make_shared<Realm>(config);
    realm->init(shared_from_this());
    m_notifier->add_realm(realm.get());
    if (config.cache) {
        m_cached_realms.push_back(realm);
    }
    return realm;
}

const Schema* RealmCoordinator::get_schema() const noexcept
{
    // FIXME: threadsafety?
    return m_cached_realms.empty() ? nullptr : m_config.schema.get();
}

uint64_t RealmCoordinator::get_schema_version() const noexcept
{
    return m_config.schema_version;
}

RealmCoordinator::RealmCoordinator() = default;
RealmCoordinator::~RealmCoordinator() = default;

void RealmCoordinator::unregister_realm(Realm* realm)
{
    bool empty = false;

    {
        std::lock_guard<std::mutex> lock(m_realm_mutex);
        m_notifier->remove_realm(realm);
        for (size_t i = 0; i < m_cached_realms.size(); ++i) {
            if (m_cached_realms[i].expired()) {
                m_cached_realms[i].swap(m_cached_realms.back());
                m_cached_realms.pop_back();
            }
        }

        // If we're empty we want to remove ourselves from the global cache, but
        // we need to release m_realm_mutex before acquiring s_coordinator_mutex
        // to avoid deadlock from acquiring locks in inconsistent orders
        empty = m_cached_realms.empty();
    }

    if (empty) {
        std::lock_guard<std::mutex> coordinator_lock(s_coordinator_mutex);
        std::lock_guard<std::mutex> lock(m_realm_mutex);
        if (m_cached_realms.empty()) {
            auto it = s_coordinators_per_path.find(m_config.path);
            // these conditions can only be false if clear_cache() was called
            if (it != s_coordinators_per_path.end() && it->second.lock().get() == this) {
                s_coordinators_per_path.erase(it);
            }
        }
    }
}

void RealmCoordinator::clear_cache()
{
    std::lock_guard<std::mutex> lock(s_coordinator_mutex);
    s_coordinators_per_path.clear();
}

void RealmCoordinator::send_commit_notifications()
{
    m_notifier->notify_others();
}
