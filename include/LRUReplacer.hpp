#pragma once

#include <algorithm>
#include <cassert>
#include <list>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>

namespace final {
/*
 * a noop lockable concept that can be used in place of std::mutex
 */
class NullLock {
public:
  void lock() {}
  void unlock() {}
  bool try_lock() { return true; }
};

/**
 * error raised when a key not in cache is passed to get()
 */
class KeyNotFound : public std::invalid_argument {
public:
  KeyNotFound() : std::invalid_argument("key_not_found") {}
};

template <typename K, typename V> struct KeyValuePair {
public:
  K key;
  V value;

  KeyValuePair(K k, V v) : key(std::move(k)), value(std::move(v)) {}
};

/**
 *	The LRU Cache class templated by
 *		Key - key type
 *		Value - value type
 *		MapType - an associative container like std::unordered_map
 *		LockType - a lock type derived from the Lock class (default:
 *NullLock = no synchronization)
 *
 *	The default NullLock based template is not thread-safe, however passing
 *Lock=std::mutex will make it
 *	thread-safe
 */
template <class Key, class Value, class Lock = NullLock,
          class Map = std::unordered_map<
              Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
class Cache {
public:
  typedef KeyValuePair<Key, Value> node_type;
  typedef std::list<KeyValuePair<Key, Value>> list_type;
  typedef Map map_type;
  typedef Lock lock_type;
  using Guard = std::lock_guard<lock_type>;

  explicit Cache(size_t maxSize = 64, size_t elasticity = 10)
      : maxSize_(maxSize) {}
  virtual ~Cache() = default;
  size_t size() const {
    Guard g(lock_);
    return cache_.size();
  }
  bool empty() const {
    Guard g(lock_);
    return cache_.empty();
  }
  void clear() {
    Guard g(lock_);
    cache_.clear();
    keys_.clear();
  }

  std::optional<KeyValuePair<Key, Value>> insert(const Key &k, Value v) {
    Guard g(lock_);
    const auto iter = cache_.find(k);
    if (iter != cache_.end()) {
      iter->second->value = v;
      keys_.splice(keys_.begin(), keys_, iter->second);
      return std::nullopt;
    }

    keys_.emplace_front(k, std::move(v));
    cache_[k] = keys_.begin();
    return prune();
  }

  std::optional<KeyValuePair<Key, Value>> emplace(const Key &k, Value &&v) {
    Guard g(lock_);
    keys_.emplace_front(k, std::move(v));
    cache_[k] = keys_.begin();
    return prune();
  }
  /**
    for backward compatibity. redirects to tryGetCopy()
   */
  bool tryGet(const Key &kIn, Value &vOut) { return tryGetCopy(kIn, vOut); }

  bool tryGetCopy(const Key &kIn, Value &vOut) {
    Guard g(lock_);
    Value tmp;
    if (!tryGetRef_nolock(kIn, tmp)) {
      return false;
    }
    vOut = tmp;
    return true;
  }

  bool tryGetRef(const Key &kIn, Value &vOut) {
    Guard g(lock_);
    return tryGetRef_nolock(kIn, vOut);
  }
  /**
   *	The const reference returned here is only
   *    guaranteed to be valid till the next insert/delete
   *  in multi-threaded apps use getCopy() to be threadsafe
   */
  const Value &getRef(const Key &k) {
    Guard g(lock_);
    return get_nolock(k);
  }

  /**
      added for backward compatibility
   */
  Value get(const Key &k) { return getCopy(k); }
  /**
   * returns a copy of the stored object (if found)
   * safe to use/recommended in multi-threaded apps
   */
  Value getCopy(const Key &k) {
    Guard g(lock_);
    return get_nolock(k);
  }

  bool remove(const Key &k) {
    Guard g(lock_);
    auto iter = cache_.find(k);
    if (iter == cache_.end()) {
      return false;
    }
    keys_.erase(iter->second);
    cache_.erase(iter);
    return true;
  }
  bool contains(const Key &k) const {
    Guard g(lock_);
    return cache_.find(k) != cache_.end();
  }

  size_t getMaxSize() const { return maxSize_; }
  // size_t getElasticity() const { return elasticity_; }
  // size_t getMaxAllowedSize() const { return maxSize_ + elasticity_; }
  size_t getMaxAllowedSize() const { return maxSize_; }
  template <typename F> void cwalk(F &f) const {
    Guard g(lock_);
    std::for_each(keys_.begin(), keys_.end(), f);
  }

protected:
  const Value &get_nolock(const Key &k) {
    const auto iter = cache_.find(k);
    if (iter == cache_.end()) {
      throw KeyNotFound();
    }
    keys_.splice(keys_.begin(), keys_, iter->second);
    return iter->second->value;
  }
  bool tryGetRef_nolock(const Key &kIn, Value &vOut) {
    const auto iter = cache_.find(kIn);
    if (iter == cache_.end()) {
      return false;
    }
    keys_.splice(keys_.begin(), keys_, iter->second);
    vOut = iter->second->value;
    return true;
  }

  // Returns the evicted key-value pair
  std::optional<KeyValuePair<Key, Value>> prune() {
    size_t maxAllowed = maxSize_;

    // If the cache is unbounded or smaller than the max size, nothing to evict
    if (maxSize_ == 0 || cache_.size() <= maxAllowed) {
      return std::nullopt; // No eviction needed
    }

    // Evict the least recently used item (from the back of the list)
    KeyValuePair<Key, Value> evictedPair = keys_.back();
    cache_.erase(evictedPair.key);
    keys_.pop_back();

    return evictedPair; // Return the evicted item
  }

private:
  // Disallow copying.
  Cache(const Cache &) = delete;
  Cache &operator=(const Cache &) = delete;

  mutable Lock lock_;
  Map cache_;
  list_type keys_;
  size_t maxSize_;
  // size_t elasticity_ = 0;
};

} // namespace final