/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/intrusive/list.hpp>

class evictable {
    friend class lru;
    using lru_link_type = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    lru_link_type _lru_link;
public:
    virtual ~evictable() {};

    virtual void on_evicted() = 0;

    void unlink_from_lru() {
        _lru_link.unlink();
    }
};

class lru {
private:
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    lru_type _list;
public:
    ~lru() {
        _list.clear_and_dispose([] (evictable* e) {
            e->on_evicted();
        });
    }
    void remove(evictable& e) {
        _list.erase(_list.iterator_to(e));
    }
    void add(evictable& e) {
        _list.push_back(e);
    }
    void evict() {
        evictable& e = _list.front();
        _list.pop_front();
        e.on_evicted();
    }
};
