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

#include "async_query.hpp"
#include "external_commit_helper.hpp"
#include "transact_log_handler.hpp"

#include <realm/commit_log.hpp>
#include <realm/group_shared.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/query.hpp>
#include <realm/table_view.hpp>

#include <cassert>

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
        m_notifier = std::make_unique<ExternalCommitHelper>(config, *this);
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

AsyncQueryCancelationToken RealmCoordinator::register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results)> fn)
{
    return r.get_realm().m_coordinator->do_register_query(r, std::move(dispatcher), std::move(fn));
}

AsyncQueryCancelationToken RealmCoordinator::do_register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results)> fn)
{
    if (m_config.read_only) {
        throw "no async read only";
    }

    std::lock_guard<std::mutex> lock(m_query_mutex);

    if (!m_query_sg) {
        m_query_history = realm::make_client_history(m_config.path, m_config.encryption_key.data());
        SharedGroup::DurabilityLevel durability = m_config.in_memory ? SharedGroup::durability_MemOnly : SharedGroup::durability_Full;
        m_query_sg = std::make_unique<SharedGroup>(*m_query_history, durability, m_config.encryption_key.data());
        m_query_sg->begin_read();

        m_advancer_history = realm::make_client_history(m_config.path, m_config.encryption_key.data());
        m_advancer_sg = std::make_unique<SharedGroup>(*m_advancer_history, durability, m_config.encryption_key.data());
        m_advancer_sg->begin_read();
    }

    auto handover = r.get_realm().m_shared_group->export_for_handover(r.get_query(), ConstSourcePayload::Copy);
    if (handover->version < m_advancer_sg->get_version_of_current_transaction()) {
        // Ensure we're holding a readlock on the oldest version we have a
        // handover object for, as handover objects don't
        m_advancer_sg->end_read();
        m_advancer_sg->begin_read(handover->version);
    }
    m_new_queries.push_back(std::make_shared<AsyncQuery>(r.get_sort(),
                                                         std::move(handover),
                                                         std::move(dispatcher),
                                                         std::move(fn),
                                                         *this));
    m_notifier->notify_others();
    return m_new_queries.back().get();
}

void RealmCoordinator::unregister_query(AsyncQuery& registration)
{
    registration.parent.do_unregister_query(registration);
}

void RealmCoordinator::do_unregister_query(AsyncQuery& registration)
{
    std::lock_guard<std::mutex> lock(m_query_mutex);
    auto it = std::find_if(m_queries.begin(), m_queries.end(),
                           [&](auto const& ptr) { return ptr.get() == &registration; });
    if (it != m_queries.end()) {
        std::iter_swap(--m_queries.end(), it);
        m_queries.pop_back();
    }
}

void RealmCoordinator::on_change()
{
    std::lock_guard<std::mutex> lock(m_query_mutex);

    if (m_queries.empty() && m_new_queries.empty()) {
        if (m_advancer_sg) {
            m_advancer_sg->end_read();
            m_advancer_sg->begin_read();
            m_query_sg->end_read();
            m_query_sg->begin_read();
        }
        return;
    }

    // Sort newly added queries by their source version, as we can't go backwards
    std::sort(m_new_queries.begin(), m_new_queries.end(), [](auto const& lft, auto const& rgt) {
        return lft->version() < rgt->version();
    });

    // Import all newly added queries to our helper SG
    for (auto& query : m_new_queries) {
        LangBindHelper::advance_read(*m_advancer_sg, *m_advancer_history, query->version());
        query->attach_to(*m_advancer_sg);
    }

    // Advance both SGs to the newest version
    LangBindHelper::advance_read(*m_advancer_sg, *m_advancer_history);
    LangBindHelper::advance_read(*m_query_sg, *m_query_history, m_advancer_sg->get_version_of_current_transaction());

    // Transfer all new queries over to the main SG
    for (auto& query : m_new_queries) {
        query->detatch();
        query->attach_to(*m_query_sg);
    }

    // Move "new queries" to the main query list
    m_queries.reserve(m_queries.size() + m_new_queries.size());
    std::move(m_new_queries.begin(), m_new_queries.end(), std::back_inserter(m_queries));
    m_new_queries.clear();

    // Run each of the queries and send the updated results
    for (auto& query : m_queries) {
        if (!query->update()) {
            continue;
        }

        if (query->get_mode() == AsyncQuery::Mode::Push) {
            std::weak_ptr<AsyncQuery> q = query;
            std::weak_ptr<RealmCoordinator> weak_self = shared_from_this();
            query->dispatch([q, weak_self] {
                auto self = weak_self.lock();
                auto query = q.lock();
                if (!query || !self) {
                    return;
                }

                std::lock_guard<std::mutex> lock(self->m_query_mutex);
                SharedRealm realm = Realm::get_shared_realm(self->m_config);
                query->deliver(realm, *realm->m_shared_group);
            });
        }
    }

    m_advancer_sg->end_read();
    m_advancer_sg->begin_read(m_query_sg->get_version_of_current_transaction());
}

static void process_available_async(Realm& realm, SharedGroup& sg, std::vector<std::shared_ptr<_impl::AsyncQuery>>& queries)
{
    // FIXME: avoid doing this under the lock by extracting the handover info
    // in advance_to_ready(), so that the next commit's queries can be run
    // at the same time as the user's blocks
    // requires holding a strong (weak?) ref to the registration
    for (auto& query : queries) {
        if (query->get_mode() == AsyncQuery::Mode::Pull) {
            query->deliver(realm.shared_from_this(), sg);
        }
    }
}

void RealmCoordinator::advance_to_ready(Realm& realm)
{
    std::lock_guard<std::mutex> lock(m_query_mutex);

    SharedGroup::VersionID version;
    for (auto& query : m_queries) {
        if (query->get_mode() == AsyncQuery::Mode::Pull) {
            version = query->version();
            if (version != SharedGroup::VersionID()) {
                break;
            }
        }
    }

    // no untargeted async queries; just advance to latest
    if (version.version == 0) {
        transaction::advance(*realm.m_shared_group, *realm.m_history, realm.m_delegate.get());
    }
    else if (version > realm.m_shared_group->get_version_of_current_transaction()) {
        transaction::advance(*realm.m_shared_group, *realm.m_history, realm.m_delegate.get(), version);
        ::process_available_async(realm, *realm.m_shared_group, m_queries);
    }
}

void RealmCoordinator::process_available_async(Realm& realm)
{
    std::lock_guard<std::mutex> lock(m_query_mutex);
    ::process_available_async(realm, *realm.m_shared_group, m_queries);
}
