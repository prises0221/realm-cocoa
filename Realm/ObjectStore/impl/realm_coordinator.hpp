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
namespace _impl {
class ExternalCommitHelper;

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

private:
    Realm::Config m_config;
    std::vector<std::weak_ptr<Realm>> m_cached_realms;
    std::mutex m_realm_mutex;
    std::unique_ptr<_impl::ExternalCommitHelper> m_notifier;
};

} // namespace _impl
} // namespace realm

#endif /* REALM_COORDINATOR_HPP */
