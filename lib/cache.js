// Simple TTL cache — Map-based, no deps. Process-wide singleton:
// only one eviction timer is created even if `new Cache()` is called
// multiple times.
let SINGLETON

class Cache {
  constructor() {
    if (SINGLETON) return SINGLETON
    this.store = new Map()
    // Periodic cleanup of expired entries every 60 seconds
    this._evictionTimer = setInterval(() => this._evictExpired(), 60000)
    this._evictionTimer.unref()
    SINGLETON = this
  }

  get(key) {
    const entry = this.store.get(key)
    if (!entry) return null
    if (Date.now() > entry.expires) {
      this.store.delete(key)
      return null
    }
    return entry.value
  }

  set(key, value, ttlMs) {
    this.store.set(key, { value, expires: Date.now() + ttlMs })
  }

  delete(key) {
    this.store.delete(key)
  }

  clear() {
    this.store.clear()
  }

  _evictExpired() {
    const now = Date.now()
    for (const [key, entry] of this.store) {
      if (now > entry.expires) this.store.delete(key)
    }
  }
}

module.exports = { Cache }
