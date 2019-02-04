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

#include <iterator>
#include <boost/intrusive/parent_from_member.hpp>
#include "utils/logalloc.hh"

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

    // Returns a reference to the reference<> pointing to this object.
    // Must be called only when is_referenced().
    reference<T>& referer();
    const reference<T>& referer() const;
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
    const T* get() const { return _ref; }
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

template<typename T>
const reference<T>& referenceable<T>::referer() const {
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

    const btree_node* parent() const {
        if (is_root()) {
            return nullptr;
        }
        return boost::intrusive::get_parent_from_member(&this->referer(),
            is_right_child() ? &btree_node::right : &btree_node::left);
    }

    btree_node* parent() {
        return const_cast<btree_node*>(std::as_const(*this).parent());
    }

    btree_node(bool is_root, bool is_right_child)
        : flags((IS_ROOT * is_root) | (IS_RIGHT_CHILD * is_right_child))
    { }

    template<typename Cloner>
    btree_node(const btree_node& other, Cloner& cloner)
        : flags(other.flags)
    {
        new (&_item.data) T(cloner(other._item.data));
    }

    template<typename... Args>
    void emplace(Args&&... args) {
        new (&_item.data) T(std::forward<Args>(args)...);
        flags |= HAS_ITEM;
    }

    btree_node(btree_node&& other) noexcept
        : referenceable<btree_node<T>>(std::move(other))
        , left(std::move(other.left))
        , right(std::move(other.right))
        , flags(other.flags)
    {
        if (other.has_item()) {
            new (&_item.data) T(std::move(other._item.data));
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

// LSA-managed ordered collection of T.
template <typename T, typename LessComparator = std::less<T>>
class btree {
    //static constexpr size_t max_node_size = 8*1024;
    //static constexpr size_t node_capacity = max_node_size / sizeof(T);
private:
    using node = btree_node<T>;
    reference<node> _root;
public:
    btree() = default;
    ~btree() { clear(); }
    btree(btree&&) noexcept = default;
public: // Iterators
    // Not stable across allocator's reference invalidation
    template<bool IsConst>
    class iterator_impl {
        using node_ptr = std::conditional_t<IsConst, const node*, node*>;
        using tree_ptr = std::conditional_t<IsConst, const btree*, btree*>;
        node_ptr _node = nullptr;
        tree_ptr _tree = nullptr;
    public:
        friend class btree;
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = std::conditional_t<IsConst, const T, T>;
        using difference_type = ssize_t;
        using pointer = value_type*;
        using reference = value_type&;
    public:
        explicit iterator_impl(tree_ptr tree, node_ptr n) : _node(n), _tree(tree) {}
        iterator_impl() = default;

        reference operator*() const { return _node->item(); }
        pointer operator->() const { return &_node->item(); }

        iterator_impl& operator++() {
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
        iterator_impl operator++(int) {
            iterator_impl it = *this;
            operator++();
            return it;
        }
        iterator_impl& operator--() {
            if (!_node) {
                _node = &*_tree->_root;
                while (_node && _node->right) {
                    _node = &*_node->right;
                }
            } else {
                if (_node->left) {
                    _node = &*_node->left;
                    while (_node->right) {
                        _node = &*_node->right;
                    }
                } else {
                    bool was_left;
                    do {
                        was_left = _node->is_left_child();
                        _node = _node->parent();
                        assert(_node); // --begin() is invalid.
                    } while (was_left);
                }
            }
            return *this;
        }
        iterator_impl operator--(int) {
            iterator_impl it = *this;
            operator--();
            return it;
        }
        bool operator==(const iterator_impl& other) const {
            return _node == other._node;
        }
        bool operator!=(const iterator_impl& other) const {
            return !(*this == other);
        }
        iterator_impl<false> unconst() const {
            return iterator_impl<false>(const_cast<btree*>(_tree), const_cast<node*>(_node));
        }
        operator iterator_impl<true>() const {
            return iterator_impl<true>(_tree, _node);
        }
        // Erases node under iterator.
        // The iterator will point to the successor.
        iterator_impl& erase() noexcept {
            node_ptr node = _node;
            operator++();
            node->erase_and_dispose();
            return *this;
        }
    };

    using iterator = iterator_impl<false>;
    using const_iterator = iterator_impl<true>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    iterator begin() { return std::as_const(*this).begin().unconst(); }
    iterator end() { return std::as_const(*this).end().unconst(); }

    const_iterator begin() const {
        const node* node = _root.get();
        if (node) {
            while (node->left) {
                node = node->left.get();
            }
        }
        return const_iterator(this, node);
    }

    // Never invalidated by mutators.
    const_iterator end() const {
        return const_iterator(this, nullptr);
    }

    const_reverse_iterator rbegin() const { return std::make_reverse_iterator(end()); }
    const_reverse_iterator rend() const { return std::make_reverse_iterator(begin()); }
    reverse_iterator rbegin() { return std::make_reverse_iterator(end()); }
    reverse_iterator rend() { return std::make_reverse_iterator(begin()); }

    static iterator iterator_to(T& item) {
        typename node::maybe_item* it = boost::intrusive::get_parent_from_member(&item, &node::maybe_item::data);
        node* n = boost::intrusive::get_parent_from_member(it, &node::_item);
        // FIXME: nullptr is not really correct, but benign if iterator is not advanced till end() and back
        return iterator(nullptr, n);
    }

    static bool is_only_member(T& item) {
        iterator it = iterator_to(item);
        return it._node->is_root() && !it._node->left && !it._node->right;
    }

    static btree& container_of_only_member(T& item) {
        iterator it = iterator_to(item);
        assert(it._node->is_root());
        return *boost::intrusive::get_parent_from_member(&it._node->referer(), &btree::_root);
    }
private:
    reference<node>& end_ref() {
        reference<node>* ref = &_root;
        if (*ref) {
            while (*ref) {
                ref = &(*ref)->right;
            }
        }
        return *ref;
    }
    template<typename... Args>
    static reference<node> make_node(Args&&... args) {
        return reference<node>(*current_allocator().construct<node>(std::forward<Args>(args)...));
    }
public: // Insertion
    class placeholder {
        btree* _tree = nullptr;
        node* _node = nullptr;
    public:
        placeholder(btree* tree, node* node) : _tree(tree), _node(node) {}
        placeholder() = default;
        placeholder(placeholder&& other)
            : _tree(other._tree)
            , _node(std::exchange(other._node, nullptr))
        { }
        placeholder(const placeholder&) = delete;
        ~placeholder() {
            if (_node) {
                _node->erase_and_dispose();
            }
        }
        template<typename... Args>
        iterator emplace(Args&&... args) {
            _node->emplace(std::forward<Args>(args)...);
            return iterator(_tree, std::exchange(_node, nullptr));
        }
        explicit operator bool() const {
            return _node;
        }
    };

    // Inserts a placeholder into the tree where the key should be.
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    //
    // Does not invalidate iterators.
    template<typename Key>
    placeholder insert_placeholder(const Key& key, LessComparator less = LessComparator()) {
        reference<node>* ref = &_root;
        bool is_root = true;
        bool is_right_child = false;

        while (*ref) {
            is_root = false;
            node& n = **ref;
            if (less(key, n.item())) {
                ref = &n.left;
                is_right_child = false;
            } else {
                ref = &n.right;
                is_right_child = true;
            }
        }

        // FIXME: rebalance
        *ref = make_node(is_root, is_right_child);
        return placeholder(this, ref->get());
    }

    // Inserts a placeholder into the tree where the key should be, unless
    // it already exists, then does nothing.
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    //
    // Does not invalidate iterators.
    template<typename Key>
    std::pair<iterator, placeholder> insert_check(const Key& key, LessComparator less = LessComparator()) {
        reference<node>* ref = &_root;
        bool is_root = true;
        bool is_right_child = false;

        while (*ref) {
            is_root = false;
            node& n = **ref;
            if (less(key, n.item())) {
                ref = &n.left;
                is_right_child = false;
            } else if (less(n.item(), key)) {
                ref = &n.right;
                is_right_child = true;
            } else {
                return std::make_pair(iterator(this, &n), placeholder());
            }
        }

        // FIXME: rebalance
        *ref = make_node(is_root, is_right_child);
        return std::make_pair(iterator(this, ref->get()), placeholder(this, ref->get()));
    }

    // Inserts a place holder for item where the key should be
    // using the given successor hint to avoid lookup when possible.
    //
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    //
    // Does not invalidate iterators.
    template<typename Key>
    placeholder insert_placeholder(iterator successor_hint, const Key& key, LessComparator less = LessComparator()) {
        // FIXME: use successor_hint
        return insert_placeholder(key, less);
    }

    // Inserts a place holder for an item right before the item referred to by the given iterator.
    //
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    //
    // Does not invalidate iterators.
    placeholder insert_before(iterator it) {
        if (!it._node) {
            return insert_back();
        } else {
            // FIXME: rebalance
            auto old_left = std::move(it._node->left);
            it._node->left = make_node(false, false);
            it._node->left->left = std::move(old_left);
            return placeholder(this, it._node->left.get());
        }
    }

    // Inserts a place holder for an item which is after all items.
    //
    // The placeholder must be either filled with emplace() or destroyed
    // before any other method is invoked on this instance.
    //
    // Does not invalidate iterators.
    placeholder insert_back() {
        // FIXME: rebalance
        auto&& ref = end_ref();
        ref = make_node(!_root, true);
        return placeholder(this, ref.get());
    }

    template<typename Cloner>
    void clone_from(const btree& other, const Cloner& cloner) {
        clear();

        const node* other_node = other._root.get();
        if (!other_node) {
            return;
        }

        _root = make_node(*other_node, cloner);
        node* this_node = _root.get();
        while (other_node->left) {
            other_node = other_node->left.get();
            this_node->left = make_node(*other_node, cloner);
            this_node = this_node->left.get();
        }

        while (other_node) {
            if (other_node->right) {
                other_node = &*other_node->right;
                this_node->right = make_node(*other_node, cloner);
                this_node = this_node->right.get();
                while (other_node->left) {
                    other_node = &*other_node->left;
                    this_node->left = make_node(*other_node, cloner);
                    this_node = this_node->left.get();
                }
            } else {
                bool was_right;
                do {
                    was_right = other_node->is_right_child();
                    other_node = other_node->parent();
                    this_node = this_node->parent();
                } while (other_node && was_right);
            }
        }
    }

    iterator insert(T item, LessComparator less = LessComparator()) {
        auto it_and_ph = insert_check(item, less);
        if (it_and_ph.second) {
            it_and_ph.second.emplace(std::move(item));
        }
        return it_and_ph.first;
    }
public: // Erasing
    void clear() {
        erase(begin(), end());
    }

    iterator erase(const_iterator it) noexcept {
        node* next = it.unconst()._node->erase_and_dispose();
        return iterator(this, next);
    }

    iterator erase(const_iterator i1, const_iterator i2) noexcept {
        while (i1 != i2) {
            auto prev = i1++;
            prev.unconst()._node->erase_and_dispose();
        }
        return i1.unconst();
    }
public: // Querying
    bool empty() const { return !_root; }

    template<typename Key>
    const_iterator lower_bound(const Key& key, LessComparator less = LessComparator()) const {
        const node* n = _root.get();
        while (n) {
            if (less(key, n->item())) {
                if (n->left) {
                    n = n->left.get();
                } else {
                    return const_iterator(this, n);
                }
            } else if (less(n->item(), key)) {
                if (n->right) {
                    n = n->right.get();
                } else {
                    auto i = const_iterator(this, n);
                    ++i;
                    return i;
                }
            } else {
                return const_iterator(this, n);
            }
        }
        return end();
    }

    template<typename Key>
    const_iterator upper_bound(const Key& key, LessComparator less = LessComparator()) const {
        const node* n = _root.get();
        while (n) {
            if (less(key, n->item())) {
                if (n->left) {
                    n = n->left.get();
                } else {
                    return const_iterator(this, n);
                }
            } else {
                if (n->right) {
                    n = n->right.get();
                } else {
                    auto i = const_iterator(this, n);
                    ++i;
                    return i;
                }
            }
        }
        return end();
    }

    template<typename Key>
    iterator lower_bound(const Key& key, LessComparator less = LessComparator()) {
        return std::as_const(*this).lower_bound(key, less).unconst();
    }

    template<typename Key>
    iterator upper_bound(const Key& key, LessComparator less = LessComparator()) {
        return std::as_const(*this).upper_bound(key, less).unconst();
    }

    template<typename Key>
    const_iterator find(const Key& key, LessComparator less = LessComparator()) const {
        const node* n = _root.get();
        while (n) {
            if (less(key, n->item())) {
                n = n->left.get();
            } else if (less(n->item(), key)) {
                n = n->right.get();
            } else {
                return const_iterator(this, n);
            }
        }
        return end();
    }

    template<typename Key>
    iterator find(const Key& key, LessComparator less = LessComparator()) {
        return std::as_const(*this).find(key, less).unconst();
    }
};
