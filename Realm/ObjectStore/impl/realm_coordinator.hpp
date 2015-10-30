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

#ifndef REALM_COORDINATOR_HPP
#define REALM_COORDINATOR_HPP

#include "shared_realm.hpp"

#include <map>
#include <memory>
#include <thread>
#include <vector>

namespace realm {
class Results;
struct AsyncQueryCancelationToken;
class ClientHistory;
class SharedGroup;

using Dispatcher = std::function<void (std::function<void()>)>;

namespace _impl {
class ExternalCommitHelper;
class AsyncQuery;

class RealmCoordinator : public std::enable_shared_from_this<RealmCoordinator> {
public:
    static std::shared_ptr<RealmCoordinator> get_coordinator(StringData path);
    static std::shared_ptr<RealmCoordinator> get_existing_coordinator(StringData path);

    std::shared_ptr<Realm> get_realm(Realm::Config config);
    const Schema* get_schema() const noexcept;
    uint64_t get_schema_version() const noexcept;

    void send_commit_notifications();

    static void clear_cache();

    RealmCoordinator();
    ~RealmCoordinator();

    void unregister_realm(Realm* realm);

    static AsyncQueryCancelationToken register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results)> fn);
    static void unregister_query(AsyncQuery& registration);

    void on_change();

    // Advance the Realm to the most recent transaction version which all async
    // work is complete for
    void advance_to_ready(Realm& realm);
    void process_available_async(Realm& realm);

private:
    Realm::Config m_config;

    std::mutex m_realm_mutex;
    std::vector<std::weak_ptr<Realm>> m_cached_realms;

    std::mutex m_query_mutex;
    std::vector<std::shared_ptr<_impl::AsyncQuery>> m_queries;
    std::vector<std::shared_ptr<_impl::AsyncQuery>> m_new_queries;

    std::unique_ptr<ClientHistory> m_query_history;
    std::unique_ptr<SharedGroup> m_query_sg;

    // SharedGroup used to advance queries in m_new_queries to the main shared
    // group's transaction version
    std::unique_ptr<ClientHistory> m_advancer_history;
    std::unique_ptr<SharedGroup> m_advancer_sg;

    // Must be last data member
    std::unique_ptr<_impl::ExternalCommitHelper> m_notifier;

    AsyncQueryCancelationToken do_register_query(const Results& r, Dispatcher dispatcher, std::function<void (Results)> fn);
    void do_unregister_query(AsyncQuery& registration);
};

} // namespace _impl
} // namespace realm

#endif /* REALM_COORDINATOR_HPP */
