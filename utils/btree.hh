/*
 * Copyright (C) 2019 ScyllaDB
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

#include "utils/logalloc.hh"
#include <boost/intrusive/parent_from_member.hpp>

template<typename T>
class reference;

template<typename T>
class referenceable {
public:
    T** _backref = nullptr;

    referenceable() = default;

    referenceable(referenceable&& other) noexcept
            : _backref(other._backref) {
        if (_backref) {
            *_backref = static_cast<T*>(this);
        }
        other._backref = nullptr;
    }

    ~referenceable() {
        if (_backref) {
            *_backref = nullptr;
        }
    }

    bool is_referenced() const {
        return _backref;
    }

    // Must be called only when is_referenced().
    reference<T>& referer();
};

template<typename T>
class reference {
public:
    T* _ref = nullptr;

    reference() = default;

    reference(referenceable<T>& ref) {
        _ref = static_cast<T*>(&ref);
        ref._backref = &_ref;
    }

    reference(reference&& other) noexcept
            : _ref(other._ref) {
        if (_ref) {
            _ref->_backref = &_ref;
        }
        other._ref = nullptr;
    }

    ~reference() {
        if (_ref) {
            current_allocator().destroy(_ref);
        }
    }

    reference& operator=(reference&& other) noexcept {
        if (_ref) {
            current_allocator().destroy(_ref);
        }
        _ref = other._ref;
        if (_ref) {
            _ref->_backref = &_ref;
        }
        other._ref = nullptr;
        return *this;
    }

    T* get() { return _ref; }
    T* operator->() { return _ref; }
    const T* operator->() const { return _ref; }
    T& operator*() { return *_ref; }
    const T& operator*() const { return *_ref; }
    explicit operator bool() const { return _ref != nullptr; }
};

template<typename T>
reference<T>& referenceable<T>::referer() {
    return *boost::intrusive::get_parent_from_member(_backref, &reference<T>::_ref);
}

template <typename T>
struct btree_node : public referenceable<btree_node<T>> {
    union maybe_item {
        maybe_item() noexcept {}
        ~maybe_item() {}
        T data;
    };

    const uint8_t IS_ROOT = 0x01;
    const uint8_t IS_RIGHT_CHILD = 0x02;
    const uint8_t HAS_ITEM = 0x04;

    // Flags which describe node's position in the tree, not its contents.
    const uint8_t POSITION_FLAGS = IS_ROOT | IS_RIGHT_CHILD;

    reference<btree_node> left;
    reference<btree_node> right;

    maybe_item _item; // FIXME: store many
    uint8_t flags;

    bool is_left_child() const { return !is_right_child(); }
    bool is_right_child() const { return flags & IS_RIGHT_CHILD; }
    bool is_root() const { return flags & IS_ROOT; }
    bool has_item() const { return flags & HAS_ITEM; }
    void set_is_root(bool is_root) const { flags = (flags & ~IS_ROOT) | (IS_ROOT * is_root); }
    void set_is_right_child(bool is_right) const { flags = (flags & ~IS_RIGHT_CHILD) | (IS_RIGHT_CHILD * is_right_child); }
    void set_position_flags(uint8_t new_flags) { flags = (flags & ~POSITION_FLAGS) | (new_flags & POSITION_FLAGS); }

    btree_node* parent() {
        if (is_root()) {
            return nullptr;
        }
        return boost::intrusive::get_parent_from_member(&this->referer(),
            is_right_child() ? &btree_node::right : &btree_node::left);
    }

    btree_node(bool is_root, bool is_right_child)
        : flags((IS_ROOT * is_root) | (IS_RIGHT_CHILD * is_right_child))
    { }

    template<typename... Args>
    void emplace(Args&&... args) {
        _item.data = T(std::forward<Args>(args)...);
        flags |= HAS_ITEM;
    }

    btree_node(btree_node&& other) noexcept
        : left(std::move(other.left))
        , right(std::move(other.right))
        , flags(other.flags)
    {
        if (other.has_item()) {
            _item.data = std::move(other._item.data);
        }
    }

    ~btree_node() {
        if (flags & HAS_ITEM) {
            _item.data.~T();
        }
    }

    T& item() { return _item.data; }
    const T& item() const { return _item.data; }

    btree_node* erase_and_dispose() noexcept {
        auto old_flags = flags;

        if (!right) {
            auto old_left = std::move(left);
            if (old_left) {
                old_left->set_position_flags(old_flags);
            }

            // Find successor
            auto next = this;
            {
                bool was_right;
                do {
                    was_right = next->is_right_child();
                    next = next->parent();
                } while (next && was_right);
            }

            this->referer() = std::move(old_left);
            return next;
        } else {
            auto old_left = std::move(left);
            auto old_right = std::move(right);
            btree_node* node = &*old_right;
            if (!node->left) {
                old_right->left = std::move(old_left);
                old_right->set_position_flags(old_flags);
                this->referer() = std::move(old_right);
                return node;
            } else {
                while (node->left) {
                    node = &*node->left;
                }
                auto node_right = std::move(node->right);
                if (node_right) {
                    node_right->set_position_flags(node->flags);
                }
                node->left = std::move(old_left);
                node->right = std::move(old_right);
                node->set_position_flags(old_flags);
                this->referer() = std::exchange(node->referer(), std::move(node_right));
                return node;
            }
        }
    }
};

template<typename T>
class rbtree_auto_unlink_hook {
public:
    void erase_and_dispose() noexcept {
        T* item = static_cast<T*>(this);
        btree_node<T>* node = boost::intrusive::get_parent_from_member(
            boost::intrusive::get_parent_from_member(item, &btree_node<T>::maybe_item::data), &btree_node<T>::item);
        node->erase_and_dispose();
    }
};

// LSA-managed ordered collection of T.
template <typename T, typename LessComparator = std::less<T>>
class btree {
    static constexpr size_t max_node_size = 8*1024;
    static constexpr size_t node_capacity = max_node_size / sizeof(T);
private:
    using node = btree_node<T>;
private:
    reference<node> root;
public:
    // Not stable across allocator's reference invalidation
    class iterator {
        node* _node = nullptr;
    public:
        friend class btree;
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = T;
        using difference_type = ssize_t;
        using pointer = T*;
        using reference = T&;
    public:
        explicit iterator(node* n) : _node(n) {}
        iterator() = default;

        T& operator*() { return _node->item(); }
        T* operator->() { return &_node->item(); }

        iterator& operator++() {
            if (_node->right) {
                _node = &*_node->right;
                while (_node->left) {
                    _node = &*_node->left;
                }
            } else {
                bool was_right;
                do {
                    was_right = _node->is_right_child();
                    _node = _node->parent();
                } while (_node && was_right);
            }
            return *this;
        }
        iterator operator++(int) {
            iterator it = *this;
            operator++();
            return it;
        }
        iterator& operator--() {
            // FIXME
            return *this;
        }
        iterator operator--(int) {
            iterator it = *this;
            operator--();
            return it;
        }
        bool operator==(const iterator& other) const {
            return _node == other._node;
        }
        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    iterator begin() {
        node* node = root.get();
        if (node) {
            while (node->left) {
                node = node->left.get();
            }
        }
        return iterator(node);
    }

    iterator end() {
        return iterator();
    }
private:
    static reference<node> make_node(bool is_root, bool is_right) {
        return reference<node>(*current_allocator().construct<node>(is_root, is_right));
    }
public:
    class placeholder {
        node* _node;
    public:
        placeholder(node* node) : _node(node) {}
        placeholder(placeholder&&) = default;
        placeholder(const placeholder&) = delete;
        ~placeholder() {
            if (_node) {
                _node->erase_and_dispose();
            }
        }
        template<typename... Args>
        iterator emplace(Args... args) {
            _node->emplace(std::forward<Args>(args)...);
            return iterator(std::exchange(_node, nullptr));
        }
    };

    // Inserts a placeholder into the tree where the key should be.
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    template<typename Key>
    placeholder insert_placeholder(const Key& key, LessComparator less) {
        reference<node>* ref = &root;
        bool is_root = true;
        bool is_right_child = false;

        while (*ref) {
            is_root = false;
            if (less(key, (*ref)->item())) {
                ref = &(*ref)->left;
                is_right_child = false;
            } else {
                ref = &(*ref)->right;
                is_right_child = true;
            }
        }

        // FIXME: rebalance
        *ref = make_node(is_root, is_right_child);
        return placeholder(ref->get());
    }

    iterator insert(T item, LessComparator less = LessComparator()) {
        placeholder ph = insert_placeholder(item, less);
        return ph.emplace(std::move(item));
    }

    template<typename Key>
    iterator lower_bound(const Key& key, LessComparator less = LessComparator()) {
        node* n = root.get();
        while (n) {
            if (less(key, n->item())) {
                if (n->left) {
                    n = n->left.get();
                } else {
                    return iterator(n);
                }
            } else if (less(n->item(), key)) {
                if (n->right) {
                    n = n->right.get();
                } else {
                    auto i = iterator(n);
                    ++i;
                    return i;
                }
            } else {
                return iterator(n);
            }
        }
        return end();
    }

    template<typename Key>
    iterator find(const Key& key, LessComparator less = LessComparator()) {
        node* n = root.get();
        while (n) {
            if (less(key, n->item())) {
                n = n->left.get();
            } else if (less(n->item(), key)) {
                n = n->right.get();
            } else {
                return iterator(n);
            }
        }
        return end();
    }

    iterator erase(iterator it) noexcept {
        node* next = it._node->erase_and_dispose();
        return iterator(next);
    }
};
