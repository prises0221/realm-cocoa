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

#include "async_query.hpp"

using namespace realm;
using namespace realm::_impl;

AsyncQuery::AsyncQuery(SortOrder sort,
                       std::unique_ptr<SharedGroup::Handover<Query>> handover,
                       Dispatcher dispatcher,
                       std::function<void (Results)> fn,
                       RealmCoordinator& parent)
: m_sort(std::move(sort))
, m_query_handover(std::move(handover))
, m_dispatcher(std::move(dispatcher))
, m_fn(std::move(fn))
, parent(parent)
{
}

void AsyncQuery::deliver(const SharedRealm& realm, SharedGroup& sg)
{
    if (!m_query_handover) {
        return;
    }
    REALM_ASSERT(m_tv_handover);
    if (m_query_handover->version < sg.get_version_of_current_transaction()) {
        // async results are stale; ignore
        return;
    }
    m_fn(Results(realm,
                 std::move(*sg.import_from_handover(std::move(m_query_handover))),
                 m_sort,
                 std::move(*sg.import_from_handover(std::move(m_tv_handover)))));
}

bool AsyncQuery::update()
{
    REALM_ASSERT(m_sg);

    if (m_tv.is_attached()) {
        if (m_tv.is_in_sync()) {
            return false;
        }
        m_tv.sync_if_needed();
    }
    else {
        m_tv = m_query->find_all();
        if (m_sort) {
            m_tv.sort(m_sort.columnIndices, m_sort.ascending);
        }
    }

    // FIXME: Stay does not work?
    m_tv_handover = m_sg->export_for_handover(m_tv, ConstSourcePayload::Copy);
    m_query_handover = m_sg->export_for_handover(*m_query, ConstSourcePayload::Stay);
    return true;
}

SharedGroup::VersionID AsyncQuery::version() const noexcept
{
    return m_query_handover ? m_query_handover->version : SharedGroup::VersionID{};
}

void AsyncQuery::attach_to(realm::SharedGroup& sg)
{
    REALM_ASSERT(!m_sg);

    m_query = sg.import_from_handover(std::move(m_query_handover));
    m_sg = &sg;
}

void AsyncQuery::detatch()
{
    REALM_ASSERT(m_sg);

    m_query_handover = m_sg->export_for_handover(*m_query, MutableSourcePayload::Move);
    m_sg = nullptr;
}

void AsyncQuery::dispatch(std::function<void ()> fn)
{
    REALM_ASSERT(m_dispatcher);
    m_dispatcher(std::move(fn));
}
