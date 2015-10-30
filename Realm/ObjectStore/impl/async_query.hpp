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

#ifndef REALM_ASYNC_QUERY_HPP
#define REALM_ASYNC_QUERY_HPP

#include "results.hpp"

#include <realm/group_shared.hpp>

#include <functional>

// todo:
// - finer-grained locking
// - test push mode
// - check thread for pull mode
// - docs and cleanup
// - more tests
// - use async stuff when possible in refresh
// - invalidate on open
// - move RLMRealm notifications to new token
// - what about autorefresh=no
// - think about reusing rlmresults instances

namespace realm {
namespace _impl {
class AsyncQuery {
public:
    RealmCoordinator& parent;

    AsyncQuery(SortOrder sort,
               std::unique_ptr<SharedGroup::Handover<Query>> handover,
               Dispatcher dispatcher,
               std::function<void (Results)> fn,
               RealmCoordinator& parent);

    void deliver(const SharedRealm& realm, SharedGroup& sg);

    bool update();

    SharedGroup::VersionID version() const noexcept;

    void attach_to(SharedGroup& sg);
    void detatch();

    enum class Mode {
        Push,
        Pull
    };

    Mode get_mode() const { return m_dispatcher ? Mode::Push : Mode::Pull; }

    void dispatch(std::function<void ()> fn);

private:
    const SortOrder m_sort;

    std::unique_ptr<SharedGroup::Handover<Query>> m_query_handover;
    std::unique_ptr<Query> m_query;

    std::unique_ptr<SharedGroup::Handover<TableView>> m_tv_handover;
    TableView m_tv;

    const Dispatcher m_dispatcher;
    const std::function<void (Results)> m_fn;

    SharedGroup* m_sg = nullptr;
};

} // namespace _impl
} // namespace realm

#endif /* REALM_ASYNC_QUERY_HPP */
